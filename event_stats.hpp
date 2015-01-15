#ifndef EVENT_STATS_HPP
#define EVENT_STATS_HPP

#include <algorithm>
#include <functional> // not1
#include <vector>
#include <set>
#include <unordered_set>

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
    void get_top(size_t k, int period_in_seconds, time_t time, ResultContainer &top_size, ResultContainer &top_freq)
    {
        int period = min(this->period, period_in_seconds);

        typedef std::set<E> SetT;
        SetT last_events;

        typedef std::unordered_set<string> RequestSetT;
        RequestSetT requests;

        typename Container::iterator it;
        E e{time - period, "", "", 0, 0};
        it = lower_bound( events.begin(), events.end(), e, &E::time_compare );

        requests.reserve( std::distance(it, events.end()) );

        for( ; it != events.end(); ++it )
        {
            auto s_it = last_events.find( *it );
            if ( s_it == last_events.end() )
            {
                E ev( *it );
                ev.freq = 1;
                last_events.insert( ev );
                requests.insert( ev.request );
            }
            else
            {
                E &ev = const_cast<E&>(*s_it);
                ev.set_size( s_it->get_size() + it->get_size() );

                if ( requests.find( ev.request ) != requests.end() )
                {
                    requests.insert( ev.request );
                    ev.set_freq( ev.get_freq() + 1 );
                }
            }
        }

        top_size = std::move( ResultContainer(last_events.begin(), last_events.end()) );
        k = min(top_size.size(), k);
        std::function<decltype(E::size_compare)> comparator_size( &E::size_compare );
        partial_sort(top_size.begin(), top_size.begin() + k, top_size.end(), std::not2(comparator_size) );
        top_size.resize(k);

        top_freq = std::move( ResultContainer(last_events.begin(), last_events.end()) );
        std::function<decltype(E::freq_compare)> comparator_freq( &E::freq_compare );
        partial_sort(top_freq.begin(), top_freq.begin() + k, top_freq.end(), std::not2(comparator_freq) );
        top_freq.resize(k);
    }

private:
    size_t max_events; // ignored
    int period;
    Container events;
};

#endif // EVENT_STATS_HPP
