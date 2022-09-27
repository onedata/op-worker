%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Macros used in workflow scheduling tests.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(WORKFLOW_SCHEDULING_TEST_COMMON_HRL).
-define(WORKFLOW_SCHEDULING_TEST_COMMON_HRL, 1).


-define(EXEMPLARY_EMPTY_STREAM, #{task_streams => #{
    1 => #{
        {1,1} => []
    }
}}).

-define(EXEMPLARY_EMPTY_STREAMS, #{task_streams => #{
    1 => #{{1,1} => []},
    3 => #{{2,2} => []},
    5 => #{{1,1} => [], {2,2} => [], {3,1} => [], {3,2} => [], {3,3} => []}
}}).

-define(EXEMPLARY_STREAM, #{task_streams => #{
    1 => #{
        {1,1} => [<<"2">>, <<"4">>]
    }
}}).

-define(EXEMPLARY_STREAMS_BASE(Extension), #{task_streams => #{
    3 => #{
        % TODO VFS-7788 - use index or id - not both
        {1,1} => [<<"1">>],
        {2,2} => [{handle_task_results_processed_for_all_items, 5}],
        {3,1} => [<<"1">>,{<<"2">>, 10},<<"3">>,<<"4">>,<<"100">>,<<"150">>],
        {3,3} => [],
        Extension
    }
}}).
-define(EXEMPLARY_STREAMS, ?EXEMPLARY_STREAMS_BASE({3,2} => [<<"100">>, handle_task_results_processed_for_all_items])).
-define(EXEMPLARY_STREAMS2, ?EXEMPLARY_STREAMS_BASE({3,2} => [<<"10">>, <<"100">>])).
-define(EXEMPLARY_STREAMS_WITH_TERMINATION_ERROR,
    ?EXEMPLARY_STREAMS_BASE({2,2} => [<<"10">>, <<"100">>, termination_error])).


-endif.