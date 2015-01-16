#ifndef EVENT_STATS_HPP
#define EVENT_STATS_HPP

#include <algorithm>
#include <functional> // not1
#include <vector>
#include <set>
#include <unordered_set>
#include <memory>

using namespace std;

#if 0
// Simple event_stats implementation, used for ADT interface specification.
// May be used as reference impl.
template<typename E>
class event_stats
{
    typedef std::vector<E> Container;
public:
    event_stats(size_t events_limit, size_t top_k, int period_in_seconds)
    : period(period_in_seconds)
    {
    }
    
    void add_event(const E &event, time_t time)
    {
        events.push_back(event);
    }

    template< typename ResultContainer >
    void get_top(size_t k, int period_in_seconds, time_t time, ResultContainer &top_size, ResultContainer &top_freq) const
    {
        int period = min(this->period, period_in_seconds);

        typedef std::set<E> SetT;
        SetT last_events;

        typedef std::unordered_set<string> RequestSetT;
        RequestSetT requests;

        typename Container::const_iterator it;
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
                ev.set_size( ev.get_size() + it->get_size() );

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
    int period;
    Container events;
};
#endif // 0

template<typename E>
class event_stats
{
    struct TopSlice
    {
        TopSlice(time_t t, size_t k)
        : time(t),
         top_size( new E[k] ),
         top_freq( new E[k] ),
         k(0),
         next(nullptr)
        {}

        time_t time;
        std::unique_ptr<E[]> top_size, top_freq;
        int k;
        TopSlice *next;
    };

public:
    event_stats(size_t events_limit, size_t top_k_, int period_in_seconds)
    : num_events(0),
     max_events(events_limit),
     top_k(top_k_),
     period(period_in_seconds),
     events( new E[events_limit] ),
     num_slices(0),
     slice_head(nullptr), slice_tail(nullptr),
     last_event_time(0)
    {
    }

    void add_event(const E &event, time_t time)
    {
        if (num_events >= max_events ||
           time - last_event_time >= 1 )
        {
            if (last_event_time)
            {
                on_timer_tick(time); // todo MT: send notification to periodic thread
                num_events = 0;
            }
            last_event_time = time;
        }
        events[num_events++] = event;
    }

    void on_timer_tick(time_t time)
    {
        // todo MT: copy events array
        erase_old_tops(time);

        int second_start = 0;
        time_t last_event_time = events[0].time;
        for(int i = 0; i < num_events; ++i)
        {
            if ( events[i].time - last_event_time >= 1 )
            {
                append_slice( build_top_slice(second_start, i, time) );
                second_start = i;
            }
        }

        if (second_start < num_events)
        {
            append_slice( build_top_slice( second_start, num_events, time ) );
        }
    }

    template< typename ResultContainer >
    void get_top(size_t k, int period_in_seconds, time_t time, ResultContainer &top_size, ResultContainer &top_freq)
    {
        int period = min(this->period, period_in_seconds);

        erase_old_tops(time);

        typedef std::set<E> SetT;
        SetT events_size, events_freq;

        typedef std::unordered_set<string> RequestSetT;
        RequestSetT requests;
        requests.reserve( k * period );

        TopSlice *slice = slice_head;
        while( slice )
        {
            for(int i = 0; i < slice->k; ++i)
            {
                const E &event_size = slice->top_size[i];
                auto s_it = events_size.find( event_size );

                if ( s_it == events_size.end() )
                {
                    events_size.insert( event_size );
                }
                else
                {
                    E &ev = const_cast<E&>(*s_it);
                    ev.set_size( ev.get_size() + event_size.get_size() );
                }

                const E &event_freq = slice->top_freq[i];
                auto f_it = events_freq.find( event_freq );

                if ( f_it == events_freq.end() )
                {
                    E ev( event_freq );
                    ev.freq = 1;
                    events_freq.insert( ev );
                    requests.insert( ev.request );
                }
                else
                {
                    E &ev = const_cast<E&>(*f_it);
                    ev.set_size( ev.get_size() + event_freq.get_size() );

                    if ( requests.find( ev.request ) != requests.end() )
                    {
                        requests.insert( ev.request );
                        ev.set_freq( ev.get_freq() + 1 );
                    }
                }
            }

            slice = slice->next;
        }

        top_size = std::move( ResultContainer(events_size.begin(), events_size.end()) );
        k = min(top_size.size(), k);
        std::function<decltype(E::size_compare)> comparator_size( &E::size_compare );
        partial_sort(top_size.begin(), top_size.begin() + k, top_size.end(), std::not2(comparator_size) );
        top_size.resize(k);

        top_freq = std::move( ResultContainer(events_freq.begin(), events_freq.end()) );
        std::function<decltype(E::freq_compare)> comparator_freq( &E::freq_compare );
        partial_sort(top_freq.begin(), top_freq.begin() + k, top_freq.end(), std::not2(comparator_freq) );
        top_freq.resize(k);
    }

private:
    void erase_old_tops(time_t time)
    {
        TopSlice *slice = slice_head;
        while( slice )
        {
            TopSlice *next = slice->next;
            if (slice->time + period >= time)
                break;

            delete slice;
            if (slice == slice_tail)
                slice_tail = nullptr;
            slice = next;
        }
        slice_head = slice;
    }

    TopSlice *build_top_slice(int start, int end, time_t time)
    {
        TopSlice *slice = new TopSlice(time, top_k);

        typedef std::set<E> SetT;
        SetT last_events;

        typedef std::unordered_set<string> RequestSetT;
        RequestSetT requests;
        requests.reserve( end - start );

        for( int i = start; i < end; ++i )
        {
            auto s_it = last_events.find( events[i] );

            if ( s_it == last_events.end() )
            {
                E ev( events[i] );
                ev.freq = 1;
                last_events.insert( ev );
                requests.insert( ev.request );
            }
            else
            {
                E &ev = const_cast<E&>(*s_it);
                ev.set_size( ev.get_size() + events[i].get_size() );

                if ( requests.find( ev.request ) != requests.end() )
                {
                    requests.insert( ev.request );
                    ev.set_freq( ev.get_freq() + 1 );
                }
            }
        }

        vector<E> top_size(last_events.begin(), last_events.end());
        int k = min(top_size.size(), top_k);
        std::function<decltype(E::size_compare)> comparator_size( &E::size_compare );
        partial_sort(top_size.begin(), top_size.begin() + k, top_size.end(), std::not2(comparator_size) );
        copy(top_size.begin(), top_size.begin() + k, &slice->top_size[0]);

        vector<E> top_freq(last_events.begin(), last_events.end());
        std::function<decltype(E::freq_compare)> comparator_freq( &E::freq_compare );
        partial_sort(top_freq.begin(), top_freq.begin() + k, top_freq.end(), std::not2(comparator_freq) );
        copy(top_freq.begin(), top_freq.begin() + k, &slice->top_freq[0]);

        slice->k = k;
        return slice;
    }

    void append_slice( TopSlice *slice )
    {
        if (slice_head == nullptr)
            slice_head = slice;
        if (slice_tail != nullptr)
            slice_tail->next = slice;
        slice_tail = slice;
    }

private:
    size_t num_events;
    size_t max_events;
    size_t top_k;
    int period;
    std::unique_ptr<E[]> events;
    int num_slices;
    TopSlice *slice_head, *slice_tail;
    time_t last_event_time;
};

#endif // EVENT_STATS_HPP
