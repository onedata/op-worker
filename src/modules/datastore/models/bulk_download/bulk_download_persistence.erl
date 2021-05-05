%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for persisting information required during bulk download.
%%% @end
%%%--------------------------------------------------------------------
-module(bulk_download_persistence).
-author("Michal Stanisz").


-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/fslogic_common.hrl").

%% API
-export([save_main_pid/2, get_main_pid/1, delete/1]).

%% datastore_model callbacks
-export([get_ctx/0]).

-record(bulk_download_persistence, {
    pid :: pid()
}).

-define(CTX, #{
    model => ?MODULE,
    fold_enabled => true,
    memory_copies => all,
    disc_driver => undefined
}).


%%%===================================================================
%%% API
%%%===================================================================

-spec save_main_pid(bulk_download:id(), pid()) -> ok.
save_main_pid(Id, Pid) ->
    ?extract_ok(datastore_model:save(?CTX, #document{key = Id, value = #bulk_download_persistence{pid = Pid}})).


-spec get_main_pid(bulk_download:id()) -> {ok, pid()} | undefined | {error, term()}.
get_main_pid(Id) ->
    case datastore_model:get(?CTX, Id) of
        {ok, #document{value = #bulk_download_persistence{pid = Pid}}} -> check_pid(Pid);
        {error, _} = Error -> Error
    end.


-spec delete(bulk_download:id()) -> ok | {error, term()}.
delete(Id) ->
    ?ok_if_not_found(datastore_model:delete(?CTX, Id)).


%%%===================================================================
%%% datastore model callbacks
%%%===================================================================

-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.


%%%===================================================================
%%% Helper functions
%%%===================================================================

%% @private
-spec check_pid(pid()) -> {ok, pid()} | {error, term()}.
check_pid(Pid) when is_pid(Pid) ->
    case is_process_alive(Pid) of
        true -> {ok, Pid};
        false -> {error, noproc}
    end.
