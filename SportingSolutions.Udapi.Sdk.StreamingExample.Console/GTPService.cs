//Copyright 2012 Spin Services Limited

//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using SportingSolutions.Udapi.Sdk.Interfaces;
using SportingSolutions.Udapi.Sdk.StreamingExample.Console.Configuration;
using SportingSolutions.Udapi.Sdk.StreamingExample.Console.Model;
using log4net;
using log4net.Appender;

namespace SportingSolutions.Udapi.Sdk.StreamingExample.Console
{
    public class GTPService : IDisposable
    {
        private readonly ILog _logger;
        private Timer _theTimer;
        private readonly ISettings _settings;
        private readonly IList<string> _sportsList;
        private readonly ConcurrentDictionary<string, StreamListener> _listeners;
        private readonly ConcurrentDictionary<string, bool> _activeFixtures;

        public GTPService(ISettings settings = null)
        {
            _settings = settings ?? Settings.Instance;
            _logger = LogManager.GetLogger(typeof(GTPService).ToString());
            _sportsList = new List<string> {"Tennis","Football","Baseball","Basketball","IceHockey","Rugby"};
            _listeners = new ConcurrentDictionary<string, StreamListener>();
            _activeFixtures = new ConcurrentDictionary<string, bool>();
        }

        public void Start()
        {
            try
            {
                _logger.Debug("Starting GTPService");
                IService theService = new Udapi.Udapi();

                _logger.Debug("UDAPI, Getting Service");
                _logger.Info("Starting timer...");
                _theTimer = new Timer(timerAutoEvent => TimerEvent(theService), null, 0, _settings.FixtureCheckerFrequency);    
            }
            catch(Exception ex)
            {
                _logger.Fatal("A fatal error has occurred and the EVenue Adapater cannot start. You can try a manual restart", ex);   
                throw;
            }
        }

        private void TimerEvent(IService theService)
        {
            try
            {
                foreach (var sport in _sportsList)
                {
                    var theFeature = theService.GetFeature(sport);
                    if (theFeature != null)
                    {
                        _logger.InfoFormat("Get the list of available fixtures for {0} from GTP", sport);
                        var fixtures = theFeature.GetResources();

                        if (fixtures != null && fixtures.Count > 0)
                        {
                            var tmpSport = sport;
                            Parallel.ForEach(fixtures, new ParallelOptions { MaxDegreeOfParallelism = 10 },
                                                fixture => ProcessFixture(fixture, tmpSport));
                        }
                        else
                        {
                            _logger.InfoFormat("There are currently no {0} fixtures in UDAPI", sport);
                        }
                    }
                    else
                    {
                        _logger.InfoFormat("Cannot find {0} in UDAPI....", sport);
                    }
                } 
            }
            catch (AggregateException aggex)
            {
                foreach (var innerException in aggex.InnerExceptions)
                {
                    _logger.Error(innerException);
                }
            }
            catch (Exception ex)
            {
                _logger.Error(ex);
            }
        }

        public static ILog AddAppender(string loggerName, IAppender appender)
        {
            var log = LogManager.GetLogger(loggerName);
            var l = (log4net.Repository.Hierarchy.Logger)log.Logger;

            l.AddAppender(appender);
            return log;
        }

        public static void SetLevel(string loggerName, string levelName)
        {
            var log = LogManager.GetLogger(loggerName);
            var l = (log4net.Repository.Hierarchy.Logger)log.Logger;

            l.Level = l.Hierarchy.LevelMap[levelName];
        }

        public static IAppender CreateRollingFileAppender(string name,string fileName)
        {
            var appender = new FileAppender();
            appender.Name = name;
            appender.File = ConfigurationManager.AppSettings["fixture.log.path"] + fileName + ".log";
            appender.AppendToFile = true;
          
            var layout = new log4net.Layout.PatternLayout();
            layout.ConversionPattern = "%date;%message%newline";
            layout.ActivateOptions();

            appender.Layout = layout;
            appender.ActivateOptions();

            return appender;
        }

        public static IAppender CreateEventLogAppender(string name)
        {
            var appender = new EventLogAppender();
            appender.Name = "EventLogAppender";

            var layout = new log4net.Layout.PatternLayout();
            layout.ConversionPattern = "%date;%message%newline";
            layout.ActivateOptions();

            appender.Layout = layout;

            appender.LogName = name;
            appender.ApplicationName = "StreamMonitor";

            appender.ActivateOptions();
            return appender;
        }

        private void ProcessFixture(IResource fixture, string sport)
        {
            if (!_activeFixtures.ContainsKey(fixture.Id) && !_listeners.ContainsKey(fixture.Id))
            {
                try
                {
                _activeFixtures.TryAdd(fixture.Id, true);
                
                SetLevel(fixture.Id,"INFO");
                var log = AddAppender(fixture.Id,CreateRollingFileAppender(fixture.Name,fixture.Name + "-" + fixture.Id));

                    var matchStatus = 0;
                    var matchSequence = 0;
                    if (fixture.Content != null)
                    {
                        matchStatus = fixture.Content.MatchStatus;
                        //Get the sequence number, if you store this to file you can check if you need to process a snapshot between restarts
                        //this can save pushing unnesasary snapshots
                        matchSequence = fixture.Content.Sequence;
                    }

                    //if not match over
                    if (matchStatus != (int)SSMatchStatus.MatchOver)
                    {
                        _logger.InfoFormat("Get UDAPI Snapshot for {0} id {1}", fixture.Name, fixture.Id);
                        var snapshotString = fixture.GetSnapshot();
                        _logger.InfoFormat("Successfully retrieved UDAPI Snapshot for {0} id {1}", fixture.Name, fixture.Id);

                        var fixtureSnapshot =
                           (Fixture)
                           JsonConvert.DeserializeObject(snapshotString, typeof(Fixture),
                                                           new JsonSerializerSettings
                                                           {
                                                               Converters =
                                                                   new List<JsonConverter> { new IsoDateTimeConverter() },
                                                               NullValueHandling = NullValueHandling.Ignore
                                                           });

                        var epoch = fixtureSnapshot.Epoch;

                    var names = ProcessSnapshot(fixtureSnapshot);

                    var streamListener = new StreamListener(fixture, epoch, sport,log,names);
                        _listeners.TryAdd(fixture.Id, streamListener);
                    }
                    else
                    {
                        _logger.InfoFormat("Fixture {0} id {1} has finished. Will not process", fixture.Name, fixture.Id);
                    }
                }
                catch (Exception ex)
                {
                    _logger.Error(string.Format("Fixture {0} id {1} There is a problem processing this fixture", fixture.Name, fixture.Id), ex);
                }
                
                bool y;
                _activeFixtures.TryRemove(fixture.Id, out y);
            }
            else
            {
                _logger.InfoFormat("Fixture {0} id {1} is currently being processed", fixture.Name, fixture.Id);
                if (_listeners.ContainsKey(fixture.Id))
                {
                    if (_listeners[fixture.Id].FixtureEnded)
                    {
                        StreamListener theListener;
                        if (_listeners.TryRemove(fixture.Id, out theListener))
                        {
                            _logger.InfoFormat("Fixture {0} id {1} is over.", fixture.Name, fixture.Id);
                        }
                        bool activeFixture;
                        _activeFixtures.TryRemove(fixture.Id, out activeFixture);
                    }
                }
            }
        }

        private List<Tuple<string, Dictionary<string, string>, Dictionary<string, string>>> ProcessSnapshot(Fixture fixture)
        {
            var name = new List<Tuple<string, Dictionary<string, string>, Dictionary<string, string>>>();

            foreach (var market in fixture.Markets)
            {
                Tuple<string, Dictionary<string, string>, Dictionary<string, string>> t;
                var marketId = market.Id;
                var marketName = market.Id;
                var marketType = "unknown";
                if (market.Tags.ContainsKey("name"))
                {
                    marketName = market.Tags["name"].ToString();
                }
                if (market.Tags.ContainsKey("type"))
                {
                    marketType = market.Tags["type"].ToString();
                }
                var marketTags = new Dictionary<string, string> { { "name", marketName }, { "type", marketType } };
                var selections = new Dictionary<string, string>();
                foreach (var selection in market.Selections)
                {
                    var selectionId = selection.Id;
                    var selectionName = selection.Id;
                    if (selection.Tags.ContainsKey("name"))
                    {
                        selectionName = selection.Tags["name"].ToString();
                    }
                    selections.Add(selectionId, selectionName);
                }
                t = new Tuple<string, Dictionary<string, string>, Dictionary<string, string>>(marketId, marketTags, selections);
                name.Add(t);
            }
            return name;
        }

        public void Stop()
        {
            if (_theTimer != null)
            {
                _theTimer.Dispose();
                _theTimer = null;

                if(_listeners != null)
                {
                    Parallel.ForEach(_listeners.Keys, new ParallelOptions {MaxDegreeOfParallelism = 10},
                                     theKey =>
                                         {
                                             var listener = _listeners[theKey];
                                             listener.StopListening();
                                         });
                }
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (_theTimer != null)
                {
                    _theTimer.Dispose();
                    _theTimer = null;
                }
            }
        }
    }
}
