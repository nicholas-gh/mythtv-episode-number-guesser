#!/usr/bin/python

import MySQLdb
import logging
import datetime
import urllib
import sys
from xml.etree import ElementTree
import random
import zipfile
import cStringIO
import unicodedata
import sqlalchemy
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relation, backref

import Levenshtein
import re
import optparse
import ds2
import numpy


"""
This is just an experiment, so is pretty messy.

It renames the titles for anything you have recordeded, to include 'Sx
Ey' at the end of the existing title. This isn't ideal, but I don't
find a better place to keep it yet... (In fact, we rename the whole
title to be the same as the thetvdb.com title.)

(I then use mythrename.pl --link ... , and I taught XBMC now to parse
the season/episode data out of the filenames.)

WARNING: If you let this guess based on only descriptions with no
other clues, it's likely to rename all your programmes totally
wrong. Even with what seems like better information (such as an
episode number in the data already) we sometimes don't manage very
well at all - I've even found programmes which were broadcast with one
title, but the show retroactively changed the title :-/ So I'm not
sure if this approach will ever work out...
"""

only_recgroups = ("Collecting",) # I only put episodes that are likely to have good thetvdb data in this recgroup
avoid_channels = (1234,)
avoid_categories = ("Movie",)
avoid_titles = tuple()
avoid_length = datetime.timedelta(hours=1.5)
tvdb_api_key = ""

mysql_connection = MySQLdb.connect (host = "",
                                    user = "mythtv",
                                    passwd = "",
                                    db = "mythconverg")

engine = sqlalchemy.create_engine('sqlite:///series.db', echo=False)


Session = sessionmaker(bind=engine)
Base = declarative_base()

# I realise sqlalchemy is overkill, but I wanted to learn it :-)
class Episode(Base):
    __tablename__ = 'episode'
    id = Column('id', Integer, primary_key=True)
    title    = Column(String(255))
    description = Column(String(4096))
    season   = Column(Integer)
    episode  = Column(Integer)
    seriesid = Column(Integer, ForeignKey('series.id'))

    def __init__(self, id, title, season, episode, description=""):
        self.id          = id
        self.title       = title
        self.season      = season
        self.episode     = episode
        self.description = description

    def __repr__(self):
        return "<Episode %s %dx%d %d>" % (repr(self.title), self.season, self.episode, self.id)

class Series(Base):
    __tablename__ = 'series'
    id = Column(Integer, primary_key=True)
    name = Column('name', String(255))
    language = Column('language', String(10))
    lastupdated = Column(Integer)

    episodes   = relation(Episode,
                          backref=backref('series', order_by=id),
                          cascade="all, delete, delete-orphan")

    def __init__(self, id, name, lastupdated, language="en"):
        self.id          = id
        self.name        = name
        self.lastupdated = lastupdated
        self.language    = language

    def season_count(self):
        return len(set([e.season for e in self.episodes]))

    def __repr__(self):
        return "<Series %s (%s) (%d episodes) %d>" % (repr(self.name),
                                                      self.language, 
                                                      len(self.episodes),
                                                      self.id)


Base.metadata.create_all(engine)

session = Session()

def fetchRecordedProgrammes(connection, title="%"):
    log = logging.getLogger("fetchRecordedProgrammes")
    cursor = connection.cursor(MySQLdb.cursors.DictCursor)
    log.debug("Querying MySQL")
    cursor.execute ("select r.recgroup, r.description, r.category, p.syndicatedepisodenumber, r.chanid, r.progstart, r.progend, r.title, r.subtitle FROM recorded r LEFT JOIN recordedprogram p ON (r.chanid = p.chanid AND r.progstart = p.starttime) where r.title like %s", title)
    for row in cursor.fetchall():
        if row['recgroup'] not in only_recgroups:
            log.debug("Skipping %s due to recgroup", row)
            continue
        if row['category'] in avoid_categories:
            log.debug("Skipping %s due to category", row)
            continue
        elif row['title'] in avoid_titles:
            log.debug("Skipping %s due to title", row)
            continue
        elif row['progend'] - row['progstart'] > avoid_length:
            log.debug("Skipping %s due to length", row)            
            continue
        if row['chanid'] in avoid_channels:
            log.debug("Skipping %s due to category", row)
            continue
        yield row
    cursor.close ()

def updateRecordedProgram(connection, row, subtitle):
    log = logging.getLogger("updateRecordedProgram")
    cursor = connection.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute ("""
         UPDATE recorded SET subtitle = %s
         WHERE title = %s and subtitle = %s and chanid = %s
       """, (subtitle, row['title'], row['subtitle'], int(row['chanid'])))
    log.debug("Number of rows updated: %d", cursor.rowcount)
    cursor.close ()

def fetchMirrored(typ, path, __mirrors={}):
    log = logging.getLogger("fetchMirrored")
    mirrors = __mirrors # static local variable :-)
    if not mirrors:
        log.debug("Findings mirrors")
        mirrors['xml']    = {}
        mirrors['zip']    = {}
        mirrors['banner'] = {}
        url = "http://www.thetvdb.com/api/%s/mirrors.xml" % tvdb_api_key
        tree = ElementTree.XML(urllib.urlopen(url).read())
        for mirror in tree.findall("Mirror"):
            _id        = int(mirror.findtext('id'))
            mirrorpath = mirror.findtext('mirrorpath')
            typemask   = int(mirror.findtext('typemask'))
            log.debug("Found mirror %s %s %s", _id, mirrorpath, typemask)
            if typemask & 1:
                mirrors['xml'][mirrorpath] = True
            if typemask & 2:
                mirrors['banner'][mirrorpath] = True
            if typemask & 4:
                mirrors['zip'][mirrorpath] = True
        log.debug("Mirrors found are %s", mirrors)
    # <mirrorpath_zip>/api/<apikey>/<seriesid>/all/<language>.zip
    mirrorpath = random.choice(mirrors[typ].keys())
    url = "%s/api/%s/%s" % (mirrorpath, tvdb_api_key, urllib.quote(path))
    log.debug("<mirrorpath_zip>/api/<apikey>/<seriesid>/all/<language>.zip")
    log.debug("Fetching %s", url)
    bytes = urllib.urlopen(url).read()
    return cStringIO.StringIO(bytes)

def fetchTime():
    log = logging.getLogger("fetchTime")
    url = "http://www.thetvdb.com/api/Updates.php?type=none"
    log.debug("Fetching %s", url)
    tree = ElementTree.XML(urllib.urlopen(url).read())
    t = int(tree.findtext("Time"))
    log.debug("Time is %d", t)
    return t

def fetchSeries(series_wanted, currenttime, language_wanted="en"):
    log = logging.getLogger("fetchSeries")
    url = "http://www.thetvdb.com/api/GetSeries.php?seriesname=%s&language=%s" % (
        urllib.quote(series_wanted),
        urllib.quote(language_wanted))
    log.debug("Fetching %s", url)
    tree = ElementTree.XML(urllib.urlopen(url).read())
    for series in tree.findall("Series"):
        SeriesName = series.findtext("SeriesName")
        seriesid   = int(series.findtext("seriesid"))
        language   = series.findtext("language")
        SeriesName_normalized =  unicodedata.normalize('NFKD', 
                                                       unicode(SeriesName)
                                                       ).encode('ASCII', 'ignore')
        log.debug("SeriesName %s normalised to %s", 
                  repr(SeriesName), 
                  repr(SeriesName_normalized))
        if SeriesName_normalized.upper() != series_wanted.upper():
            log.debug("Rejecting %s as match for %s", SeriesName, series_wanted)
            continue
        if language.upper() != language_wanted.upper():
            log.debug("Rejecting %s as match for %s", language, language_wanted)
            continue
        return Series(seriesid, series_wanted, currenttime, language)

def getSeries(session, title, currenttime=fetchTime(), language="en", _updates_available={}, offline=True):
    log = logging.getLogger("getSeries")
    log.debug("Getting hold of series %s", repr(title))
    dbseries = session.query(Series).filter_by(name=title).first()
    if dbseries:
        log.debug("Found in database %s", dbseries)
        log.debug("Was stored to database at %d", dbseries.lastupdated)
        if offline:
            log.debug("Trying to run offline, so not checking for updates")
            return dbseries
        if _updates_available:
            if _updates_available["since"] > dbseries.lastupdated:
                log.debug("We only have a list of updates since %d, so fetching again.", 
                          _updates_available["since"])
            for key in _updates_available.keys():
                del _updates_available[key]
        if not _updates_available:
            url = "http://www.thetvdb.com/api/Updates.php?type=all&time=%d" % dbseries.lastupdated
            log.debug("Fetching %s", url)
            tree = ElementTree.XML(urllib.urlopen(url).read())
            _updates_available["since"]   = dbseries.lastupdated
            _updates_available["to"]      = int(tree.findtext("Time"))
            _updates_available["updates"] = [s.text for s in tree.findall("Series")]
            log.debug(_updates_available)
        if dbseries.id in _updates_available["updates"]:
            log.debug("Series %d has an update available" % dbseries)
            log.debug("Deleting our copy")
            session.delete(dbseries)
        else:
            log.debug("No update available, so returning %s", dbseries)
            return dbseries
    log.debug("Fetching from Internet")
    series   = fetchSeries(title, currenttime, language)
    if series:
        dbseries = session.query(Series).filter_by(id=series.id).first()
        if dbseries:
            log.warning("We failed to find series in db, downloaded it, now actually we did have it.")
            return dbseries
        session.save(series)
        serieszip = zipfile.ZipFile(
            fetchMirrored("zip", "series/%s/all/%s.zip" % (series.id,
                                                           series.language)))
        seriesxml = ElementTree.XML(serieszip.read("%s.xml" % language))
        for episodexml in seriesxml.findall("Episode"):
            episode = Episode(int(episodexml.findtext("id")),
                              episodexml.findtext("EpisodeName"),
                              int(episodexml.findtext("SeasonNumber")),
                              int(episodexml.findtext("EpisodeNumber")),
                              description=episodexml.findtext("Overview"))
            series.episodes.append(episode)
        session.flush()
        return series

syndicatedepisodenumber_re = re.compile(".*E([0-9]+)$")
episode_subtitle_encoding_re = re.compile(".*S([0-9]+) ?E([0-9]+).*")

parser = optparse.OptionParser()
parser.add_option("-d", "--debug", dest="debug",
                  action="store_true", default=False,
                  help="Debugging mode")
parser.add_option("--dry-run", dest="dryrun",
                  action="store_true", default=False,
                  help="Debugging mode")
parser.add_option("-o", "--offline", dest="offline",
                  action="store_true", default=False,
                  help="Mostly offline mode")
parser.add_option("--descriptions", dest="fulldescriptions",
                  action="store_true", default=False,
                  help="Allow to search based only on description matching")
parser.add_option("-t", "--title", dest="title",
                  default="%",
                  help="Only do title TITLE")

(options, args) = parser.parse_args()

if options.debug:
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.INFO)

log = logging.getLogger()
log.debug("Starting up.")
for recordedprogramme in fetchRecordedProgrammes(mysql_connection, title=options.title):
    log.debug("Investigating %s", recordedprogramme)
    if not (recordedprogramme['subtitle'] or \
                recordedprogramme['syndicatedepisodenumber'] or \
                options.fulldescriptions):
        log.warning("Can't match %s without subtitle or syndicatedepisodenumber yet",
                    repr(recordedprogramme['title']))
        # we don't support date based matching or anything yet
        continue

    if episode_subtitle_encoding_re.match(recordedprogramme['subtitle']):
        # already has information in
        log.debug("Already contains season/episode in subtitle, so skipping")
        continue

    series = getSeries(session, recordedprogramme['title'], offline=options.offline)
    if not series:
        log.warning("Can't find series %s", repr(recordedprogramme['title']))
        continue
    log.debug("Found series %s", series)

    best_match_e = None
    episode_options = []

    if recordedprogramme['syndicatedepisodenumber']:
        log.debug("Going to guess based on syndicatedepisodenumber %s",
                  recordedprogramme['syndicatedepisodenumber'])
        m = syndicatedepisodenumber_re.match(recordedprogramme['syndicatedepisodenumber'])
        if m:
            known_episode_number = int(m.group(1))
            log.debug("TV recording says episode %d", known_episode_number)
            episode_options = [episode for episode in series.episodes if 
                       episode.episode == known_episode_number]
        else:
            log.debug("Failed to understand %s",
                      repr(recordedprogramme['syndicatedepisodenumber']))

    if not episode_options and recordedprogramme['subtitle']:
        log.debug("Haven't managed to get a set of episode options, so going for whole series as we have a subtitle")
        episode_options = series.episodes[:]
    if not episode_options and options.fulldescriptions:
        log.debug("Haven't managed to get a set of episodes options, but going to use description matching against all seasons")
        episode_options = series.episodes[:]
        
    log.debug("Episode options are %s", episode_options)

    if len(episode_options) == 1 and series.season_count() == 1:
        log.debug("Only one episode option and only one season, so going with that.")
        best_match_e = episode_options[0]

    if best_match_e is None and recordedprogramme['subtitle']:
        log.debug("Going to guess based on subtitle matching")
        best_match_r = 0.0
        for possible_episode in episode_options:
            log.debug("Checking option %s", possible_episode)
            if isinstance(possible_episode.title, type(u'')):
                subtitle_str = possible_episode.title.encode('utf8')
            else:
                subtitle_str = possible_episode.title
            r = Levenshtein.ratio(subtitle_str, recordedprogramme['subtitle'])
            log.debug("Similarity score %s", r)
            if r > 0.8 and r > best_match_r:
                best_match_r = r
                best_match_e = possible_episode
                log.debug("Found better match for %s: %s", 
                          recordedprogramme['subtitle'],
                          best_match_e)

    if best_match_e is None and len(recordedprogramme['description']) > 150:
        log.debug("Going for description matching")
        log.debug("Recording description is: %s", repr(recordedprogramme['description']))
        best_match_r = 0.0
        for possible_episode in episode_options:
            log.debug("%s: %s", possible_episode, repr(possible_episode.description))
            if len(possible_episode.description) > 150:
                r = ds2.compare(possible_episode.description,
                                repr(recordedprogramme['description']))
                log.debug("Similarity score %s", r)
                if r is not numpy.nan and r > best_match_r:
                    best_match_r = r
                    best_match_e = possible_episode
                    log.debug("Found better match for %s: %s", 
                              recordedprogramme['subtitle'],
                              best_match_e)

    
    if not best_match_e:
        log.warning("Couldn't find a match for %s: %s",
                    repr(recordedprogramme['title']),
                    repr(recordedprogramme['subtitle']))
        continue
    log.debug("Best match was %s", best_match_e)
    if best_match_e.title:
        best_title = "%s " % best_match_e.title
    else:
        best_title = ""
    new_subtitle = "%sS%d E%d" % (best_title,
                                  best_match_e.season,
                                  best_match_e.episode)
    log.info("New title for %s: %s is %s", 
             repr(recordedprogramme['title']),
             repr(recordedprogramme['subtitle']),
             repr(new_subtitle))
    if not options.dryrun:
        updateRecordedProgram(mysql_connection, recordedprogramme, new_subtitle)
    else:
        log.debug("Dryrun - not updating MySQL")
        

session.commit()
mysql_connection.commit() # oh yeah, mysql doesn't support transactions by default anyway...
mysql_connection.close()
