#ifndef EVENT_STATS_HPP
#define EVENT_STATS_HPP

#include <algorithm>
#include <functional> // not1
#include <vector>
#include <set>

using namespace std;


// Simple event_stats implementation, used for ADT interface specification.
// May be used as reference impl.
template<typename E>
class event_stats
{
    typedef std::vector<E> Container;
public:
    event_stats(size_t events_limit, int period_in_seconds)
    : max_events(events_limit),
     period(period_in_seconds)
    {
    }
    
    void add_event(const E &event, time_t time)
    {
        events.push_back(event);
    }

    template< typename ResultContainer >
    void get_top(size_t k, int period_in_seconds, time_t time, ResultContainer &result)
    {
        int period = min(this->period, period_in_seconds);

        typedef std::set<E> SetType;
        SetType last_events;

        typename Container::const_iterator it;
        E e{time - period, "", 0};
        it = lower_bound( events.begin(), events.end(), e, &E::time_compare );
        for( ; it != events.end(); ++it )
        {
            auto s_it = last_events.find( *it );
            if ( s_it == last_events.end() )
            {
                last_events.insert( *it );
            }
            else
            {
                E &e = const_cast<E&>(*s_it);
                e.set_size( s_it->get_size() + it->get_size() );
            }
        }

        result = std::move( ResultContainer(last_events.begin(), last_events.end()) );
        k = min(result.size(), k);
        std::function<bool(const E&, const E&)> comparator( &E::size_compare );
        partial_sort(result.begin(), result.begin() + k, result.end(), std::not2(comparator) );
        result.resize(k);
    }

private:
    size_t max_events; // ignored
    int period;
    Container events;
};

#endif // EVENT_STATS_HPP
