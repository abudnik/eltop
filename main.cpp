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
    string key;
    mutable uint64_t size;

    uint64_t get_size() const { return size; }
    void set_size(uint64_t size) const { this->size = size; }

    static bool size_compare(const Event &e1, const Event &e2) { return e1.size < e2.size; }
    static bool time_compare(const Event &e1, time_t val) { return e1.time < val; }
    bool operator < (const Event &e) const { return key.compare( e.key ) < 0; }
};

std::ostream& operator << (std::ostream& os, const Event &ev)
{
    os << "event = {key: " << ev.key << ", time: " << ev.time << ", size: " << ev.size << "}";
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
        for( auto &observer : observers_ )
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
    : stats_( event_stats )
    {}

private:
    // IObserver
    virtual void NotifyObserver( const Event &event )
    {
        typedef vector<Event> EventContainer;
        EventContainer top;
        stats_->get_top(50, 5*60, event.time, top);

        cout << "time_t = " << event.time << '\n';
        unsigned i = 0;
        for( const auto e : top )
        {
            cout << i++ << ' ' << e << endl;
        }
    }

private:
    EventStatsPtr stats_;
};

class EventParser : public Observable
{
public:
    void Parse(const char *file_name)
    {
        ifstream file(file_name);
        string line;
        unsigned line_num = 0;
        struct std::tm tm;

        typedef boost::char_separator<char> Separator;
        Separator sep(",");

        Event event;
        while( getline( file, line ) )
        {
            boost::tokenizer<Separator> tokens(line, sep);
            int tok_number = 0;
            for( auto& t : tokens ) {
                switch(tok_number) {
                    case 0:
                        strptime(t.c_str(), "%Y-%m-%d %H:%M:%S", &tm);
                        event.time = mktime(&tm);
                        break;
                    case 1:
                        event.key = move(t);
                        break;
                    case 2:
                        event.size = stoull(t);
                        break;
                }
                ++tok_number;
            }

            if ( tok_number == 3 ) {
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
