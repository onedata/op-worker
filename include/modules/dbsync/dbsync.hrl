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

-record(synchronization_params, {
    mode :: dbsync_state:synchronization_mode(),
    target_seq :: couchbase_changes:seq(),  % Seq number that ends particular mode of synchronization (depending on mode,
                                            % mode may be changed or params may be deleted when this seq number is reached).
    included_mutators :: dbsync_in_stream:mutators()    % Only documents of included_mutators are resynchronized - documents
                                                        % mutated by other providers are ignored during resynchronization.
}).

-endif.