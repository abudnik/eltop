#define _XOPEN_SOURCE
#include <iostream>
#include <fstream>
#include <vector>
#include <ctime>
#include <cstring>
#include <boost/tokenizer.hpp>
#include "event_stats.hpp"

using namespace std;


struct Event
{
    time_t time;
    string request;
    string key;
    uint64_t size;
    uint64_t freq;

    uint64_t get_size() const { return size; }
    void set_size(uint64_t size) { this->size = size; }

    uint64_t get_freq() const { return freq; }
    void set_freq(uint64_t freq) { this->freq = freq; }

    static bool size_compare(const Event &e1, const Event &e2) { return e1.size < e2.size; }
    static bool time_compare(const Event &e1, const Event &e2) { return e1.time < e2.time; }
    static bool freq_compare(const Event &e1, const Event &e2) { return e1.freq < e2.freq; }
    bool operator < (const Event &e) const { return key.compare( e.key ) < 0; }
};

std::ostream& operator << (std::ostream& os, const Event &e)
{
    os << "event = {key: " << e.key << ", request: " << e.request
       << ", freq: " << e.freq << ", time: " << e.time << ", size: " << e.size << "}";
    return os;
}


typedef event_stats<Event> EventStats;
typedef EventStats* EventStatsPtr;

struct IObserver
{
    virtual void NotifyObserver( const Event &event ) = 0;
    virtual ~IObserver() {}
};

struct IObservable
{
    virtual void Subscribe( IObserver *observer ) = 0;
    virtual void NotifyAll( const Event &e ) = 0;
};

class Observable : virtual public IObservable
{
    typedef vector<IObserver *> Container;
public:
    virtual void Subscribe( IObserver *observer )
    {
        observers_.push_back( observer );
    }
    virtual void NotifyAll( const Event &event )
    {
        for( auto observer : observers_ )
        {
            observer->NotifyObserver( event );
        }
    }
private:
    Container observers_;
};

class EventSerializationHandler : public IObserver
{
public:
    EventSerializationHandler( EventStatsPtr event_stats )
    : stats_( event_stats )
    {}

private:
    // IObserver
    virtual void NotifyObserver( const Event &event )
    {
        stats_->add_event( event, event.time );
    }

private:
    EventStatsPtr stats_;
};

class EventStatisticsHandler : public IObserver
{
public:
    EventStatisticsHandler( EventStatsPtr event_stats )
    : stats_( event_stats ),
     last_event_time_(0),
     max_freq(0)
    {}

private:
    // IObserver
    virtual void NotifyObserver( const Event &event )
    {
        if (!last_event_time_)
            last_event_time_ = event.time;

        if ( event.time - last_event_time_ >= 1 ) // every second
        {
            // if more than one second elapsed since last event, then insert fake events
            // and GetTop for those 'missed' seconds
            const int num_fake_events = event.time - last_event_time_ - 1;
            for(int i = 1; i <= num_fake_events; ++i)
            {
                const time_t missed_second = last_event_time_ + i;
                GetTop( missed_second );
            }

            GetTop(event.time);
            last_event_time_ = event.time;
        }
    }

    void GetTop( time_t current_time )
    {
        typedef vector<Event> EventContainer;
        EventContainer top_size, top_freq;
        stats_->get_top(50, 5*60, current_time, top_size, top_freq);

        cout << "time_t = " << current_time << '\n';
        
        cout << "top by size" << '\n';

        int intersect = 0;
        int i = 0;
        for( const auto& e : top_size )
        {
            cout << i++ << ' ' << e << '\n';

            for( const auto& ev : top_freq ) {
                if (e.key == ev.key)
                {
                    ++intersect;
                    break;
                }
            }
        }

        cout << "top by frequency" << '\n';
        i = 0;
        for( const auto& e : top_freq )
        {
            cout << i++ << ' ' << e << '\n';
            if (e.freq > max_freq) max_freq = e.freq;
        }

        cout << "intersect= " << intersect << '\n';
        cout << "max_freq= " << max_freq << endl;
    }

private:
    EventStatsPtr stats_;
    time_t last_event_time_;
    uint64_t max_freq;
};

class EventParser : public Observable
{
public:
    void Parse(const char *file_name)
    {
        ifstream file(file_name);
        string line;
        unsigned line_num = 0;
        size_t pos;
        struct std::tm tm;

        typedef boost::char_separator<char> Separator;
        Separator sep(",");

        Event event;
        event.freq = 0;
        while( getline( file, line ) )
        {
            boost::tokenizer<Separator> tokens(line, sep);
            int tok_number = 0;
            for( auto& t : tokens ) {
                switch(tok_number) {
                    case 0:
                        memset(&tm, 0, sizeof(struct std::tm));
                        strptime(t.c_str(), "%Y-%m-%d %H:%M:%S", &tm);
                        event.time = mktime(&tm);
                        break;
                    case 1:
                        pos = t.find('/');
                        if (pos != string::npos)
                            event.request = std::move( string(t,0,pos) );
                        else
                            event.request.clear();
                        break;
                    case 2:
                        event.key = move(t);
                        break;
                    case 3:
                        event.size = stoull(t);
                        break;
                }
                ++tok_number;
            }

            if ( tok_number == 4 ) {
                NotifyAll( event );
            } else {
                cerr << "failed parse line: " << line_num << ": " << line << endl;
                break;
            }

            ++line_num;
        }
    }
};


int main(int argc, const char* argv[])
{
    if (argc < 2) {
        cerr << "file name argument expected" << endl;
        return 1;
    }

    try
    {
        EventStats stats(10 * 1000, 5 * 60);
        EventSerializationHandler evSerialization(&stats);
        EventStatisticsHandler evStats(&stats);

        EventParser parser;
        parser.Subscribe( &evSerialization );
        parser.Subscribe( &evStats );
        parser.Parse(argv[1]);
    }
    catch(exception &e)
    {
        cerr << e.what() << endl;
        return 1;
    }

    return 0;
}
