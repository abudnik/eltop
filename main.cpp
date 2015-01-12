#define _XOPEN_SOURCE
#include <iostream>
#include <fstream>
#include <vector>
#include <ctime>
#include <cstring>
#include <boost/tokenizer.hpp>

using namespace std;


struct Event
{
    time_t time;
    string key;
    uint64_t size;
};

class Observer
{
public:
    virtual void NotifyObserver( Event &event ) = 0;
    virtual ~Observer() {}
};

struct IObservable
{
    virtual void Subscribe( Observer *observer ) = 0;
    virtual void NotifyAll( Event &e ) = 0;
};

class Observable : virtual public IObservable
{
    typedef vector<Observer *> Container;
public:
    virtual void Subscribe( Observer *observer )
    {
        observers_.push_back( observer );
    }
    virtual void NotifyAll( Event &event )
    {
        for( auto &observer : observers_ )
        {
            observer->NotifyObserver( event );
        }
    }
private:
    Container observers_;
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
        EventParser parser;
        parser.Parse(argv[1]);
    }
    catch(exception &e)
    {
        cerr << e.what() << endl;
        return 1;
    }

    return 0;
}
