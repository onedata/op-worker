%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Macros and records used in modules connected to dbsync.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(DBSYNC_HRL).
-define(DBSYNC_HRL, 1).

% Special values for dbsync_in_stream.mutators() type
-define(ALL_MUTATORS, [<<"all">>]).
-define(ALL_MUTATORS_EXCEPT_SENDER, [<<"all_mutators_except_sender">>]).

-record(resynchronization_params, {
  final_seq :: couchbase_changes:seq(),
  included_mutators :: dbsync_in_stream:mutators()
}).

-endif.