%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module used for performing requests to URL callbacks
%%% notifying about operations executed on archives.
%%% @end
%%%-------------------------------------------------------------------
-module(archivisation_callback).
-author("Jakub Kudzia").

-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/http/codes.hrl").

%% API
-export([
    notify_preserved/3, notify_preservation_failed/4,
    notify_purged/3
]).

-define(PRESERVATION, preservation).
-define(PURGING, purging).

-type operation() :: ?PRESERVATION | ?PURGING.
-type error_description() :: binary() | null.

-define(MAX_RETRIES, 30).
-define(MAX_INITIAL_INTERVAL, timer:seconds(1)).
-define(INITIAL_INTERVAL(), rand:uniform(?MAX_INITIAL_INTERVAL)).
-define(MAX_INTERVAL, timer:hours(4)).

-define(HEADERS, #{?HDR_CONTENT_TYPE => <<"application/json">>}).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec notify_preserved(archive:id(), dataset:id(), archive:callback()) -> ok.
notify_preserved(ArchiveId, DatasetId, CallbackUrl) ->
    notify_archive_callback(ArchiveId, DatasetId, CallbackUrl, ?PRESERVATION, null).


-spec notify_preservation_failed(archive:id(), dataset:id(), archive:callback(), binary()) -> ok.
notify_preservation_failed(ArchiveId, DatasetId, CallbackUrl, ErrorDescription) ->
    notify_archive_callback(ArchiveId, DatasetId, CallbackUrl, ?PRESERVATION, ErrorDescription).


-spec notify_purged(archive:id(), dataset:id(), archive:callback()) -> ok.
notify_purged(ArchiveId, DatasetId, CallbackUrl) ->
    notify_archive_callback(ArchiveId, DatasetId, CallbackUrl, ?PURGING, null).

%%%===================================================================
%%% Internal functions
%%%===================================================================


-spec notify_archive_callback(archive:id(), dataset:id(), archive:callback(), operation(), error_description()) -> ok.
notify_archive_callback(_ArchiveId, _DatasetId, undefined, _Operation, _ErrorDescription) ->
    ok;
notify_archive_callback(ArchiveId, DatasetId, CallbackUrl, Operation, ErrorDescription) ->
    spawn(fun() ->
        do_notify_or_retry(ArchiveId, DatasetId, CallbackUrl, Operation, ErrorDescription, ?INITIAL_INTERVAL(), ?MAX_RETRIES)
    end).


-spec do_notify_or_retry(archive:id(), dataset:id(), archive:callback(), operation(), error_description(), 
    non_neg_integer(), non_neg_integer()) -> ok.
do_notify_or_retry(_ArchiveId, _DatasetId, _CallbackUrl, _Operation, _ErrorDescription, _Sleep, 0) ->
    ok;
do_notify_or_retry(ArchiveId, DatasetId, CallbackUrl, Operation, ErrorDescription, Sleep, RetriesLeft) ->
    try
        {ok, ResponseCode, _, _} =
            http_client:post(CallbackUrl, ?HEADERS, prepare_body(ArchiveId, DatasetId, ErrorDescription)),
        case http_utils:is_success_code(ResponseCode) of
            true ->
                ok;
            false ->
                ?error(
                    "Calling URL callback ~s, after ~s of "
                    "archive ~s created from dataset ~s failed with ~p response code.~n"
                    "Next retry in ~p seconds. Number of retries left: ~p",
                    [CallbackUrl, Operation, ArchiveId, DatasetId, ResponseCode]),
                wait_and_retry(ArchiveId, DatasetId, CallbackUrl, Operation, ErrorDescription, Sleep, RetriesLeft - 1)
        end
    catch
        Type:Reason ->
            ?error_stacktrace(
                "Calling URL callback ~s, after ~s of "
                "archive ~s created from dataset ~s, failed due to ~p:~p.~n"
                "Next retry in ~p seconds. Number of retries left: ~p",
                [CallbackUrl, Operation, ArchiveId, DatasetId, Type, Reason]),
            wait_and_retry(ArchiveId, DatasetId, CallbackUrl, Operation, ErrorDescription, Sleep, RetriesLeft - 1)
    end.


-spec wait_and_retry(archive:id(), dataset:id(), archive:callback(), operation(), error_description(),
    non_neg_integer(), non_neg_integer()) -> ok.
wait_and_retry(ArchiveId, DatasetId, CallbackUrl, Operation, ErrorDescription, Sleep, RetriesLeft) ->
    timer:sleep(Sleep),
    NextSleep = min(2 * Sleep, ?MAX_INTERVAL),
    do_notify_or_retry(ArchiveId, DatasetId, CallbackUrl, Operation, ErrorDescription, NextSleep, RetriesLeft - 1).


-spec prepare_body(archive:id(), dataset:id(), error_description()) -> binary().
prepare_body(ArchiveId, DatasetId, ErrorDescription) ->
    json_utils:encode(#{
        <<"archiveId">> => ArchiveId,
        <<"datasetId">> => DatasetId,
        <<"error">> => ErrorDescription
    }).