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
    mode :: resynchronization | initial_sync | initial_sync_to_repeat,
    target_seq :: couchbase_changes:seq(),  % Seq number that ends resynchronization (resynchronization params are
                                            % deleted when this seq number is reached). It is equal to current sequence
                                            % number of stream at the moment of resynchronization start
                                            % (see dbsync_state:resynchronize_stream/3).
    included_mutators :: dbsync_in_stream:mutators()    % Only documents of included_mutators are resynchronized - documents
                                                        % mutated by other providers are ignored during resynchronization.
}).

-endif.