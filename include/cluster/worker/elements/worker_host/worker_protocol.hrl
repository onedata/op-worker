%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% The protocol between worker_host and the rest of the world
%%% @end
%%%-------------------------------------------------------------------

-ifndef(WORKER_PROTOCOL_HRL).
-define(WORKER_PROTOCOL_HRL, 1).

-type process_ref() :: undefined | {proc, pid()} | {gen_serv, pid() | atom()}.

-record(worker_request, {
    id = undefined :: term(),
    req = undefined :: term(),
    reply_to = undefined :: process_ref()
}).
-record(worker_answer, {
    id = undefined :: term(),
    response = undefined :: term()
}).

-endif.