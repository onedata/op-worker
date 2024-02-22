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
-module(bulk_download_task).
-author("Michal Stanisz").


-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([save/3, get_main_pid/1, get_session_id/1, delete/1]).

%% datastore_model callbacks
-export([get_ctx/0]).

-record(bulk_download_task, {
    pid :: pid(),
    session_id :: session:id()
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

-spec save(bulk_download:id(), pid(), session:id()) -> ok.
save(BulkDownloadId, Pid, SessionId) ->
    ?extract_ok(datastore_model:save(?CTX, #document{
        key = BulkDownloadId, value = #bulk_download_task{
            pid = Pid,
            session_id = SessionId
        }
    })).


-spec get_main_pid(bulk_download:id()) -> {ok, pid()} | {error, term()}.
get_main_pid(BulkDownloadId) ->
    case lookup(BulkDownloadId) of
        {ok, #bulk_download_task{pid = Pid}} ->
            {ok, Pid};
        {error, _} = Error ->
            Error
    end.


-spec get_session_id(bulk_download:id()) -> {ok, session:id()} | {error, term()}.
get_session_id(BulkDownloadId) ->
    case lookup(BulkDownloadId) of
        {ok, #bulk_download_task{session_id = SessionId}} ->
            {ok, SessionId};
        {error, _} = Error ->
            Error
    end.


-spec delete(bulk_download:id()) -> ok | {error, term()}.
delete(BulkDownloadId) ->
    ?ok_if_not_found(datastore_model:delete(?CTX, BulkDownloadId)).


%%%===================================================================
%%% datastore model callbacks
%%%===================================================================

-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

%%%===================================================================
%%% helpers
%%%===================================================================

%% @private
-spec lookup(bulk_download:id()) -> ok.
lookup(BulkDownloadId) ->
    case datastore_model:get(?CTX, BulkDownloadId) of
        {ok, #document{value = #bulk_download_task{pid = Pid} = Record}} ->
            case is_process_alive(Pid) of
                true ->
                    {ok, Record};
                false ->
                    delete(BulkDownloadId),
                    ?ERROR_NOT_FOUND
            end;
        {error, _} = Error ->
            Error
    end.
