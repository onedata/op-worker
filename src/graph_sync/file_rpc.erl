%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This model implements functions handling gs rpc for files.
%%% @end
%%%-------------------------------------------------------------------
-module(file_rpc).
-author("Bartosz Walkowicz").

-include("op_logic.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").

%% API
-export([handle/3]).


%%%===================================================================
%%% API
%%%===================================================================


-spec handle(aai:auth(), gs_protocol:rpc_function(), gs_protocol:rpc_args()) ->
    gs_protocol:rpc_result().
handle(Auth, RpcFun, Data) ->
    try
        handle_internal(Auth, RpcFun, Data)
    catch
        throw:Error = {error, _} ->
            Error;
        Type:Reason ->
            ?error_stacktrace("Unexpected error while processing gs file rpc "
                              "request - ~p:~p", [Type, Reason]),
            ?ERROR_INTERNAL_SERVER_ERROR
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec handle_internal(aai:auth(), gs_protocol:rpc_function(), gs_protocol:rpc_args()) ->
    gs_protocol:rpc_result().
handle_internal(Auth, <<"getDirChildren">>, Data) ->
    ls(Auth, Data);
handle_internal(Auth, <<"initializeFileUpload">>, Data) ->
    register_file_upload(Auth, Data);
handle_internal(Auth, <<"finalizeFileUpload">>, Data) ->
    deregister_file_upload(Auth, Data);
handle_internal(Auth, <<"getFileDownloadUrl">>, Data) ->
    get_file_download_url(Auth, Data);
handle_internal(Auth, <<"moveFile">>, Data) ->
    move(Auth, Data);
handle_internal(Auth, <<"copyFile">>, Data) ->
    copy(Auth, Data);
handle_internal(_, _, _) ->
    ?ERROR_RPC_UNDEFINED.


%% @private
-spec ls(aai:auth(), gs_protocol:rpc_args()) -> gs_protocol:rpc_result().
ls(?USER(_UserId, SessionId) = Auth, Data) ->
    SanitizedData = op_sanitizer:sanitize_data(Data, #{
        required => #{
            <<"guid">> => {binary, non_empty},
            <<"limit">> => {integer, {not_lower_than, 1}}
        },
        optional => #{
            <<"index">> => {any, fun
                (null) ->
                    {true, undefined};
                (undefined) ->
                    true;
                (<<>>) ->
                    throw(?ERROR_BAD_VALUE_EMPTY(<<"index">>));
                (IndexBin) when is_binary(IndexBin) ->
                    true;
                (_) ->
                    false
            end},
            <<"offset">> => {integer, any}
        }
    }),
    FileGuid = maps:get(<<"guid">>, SanitizedData),
    Limit = maps:get(<<"limit">>, SanitizedData),
    StartId = maps:get(<<"index">>, SanitizedData, undefined),
    Offset = maps:get(<<"offset">>, SanitizedData, 0),

    assert_space_membership_and_local_support(Auth, FileGuid),

    case lfm:ls(SessionId, {guid, FileGuid}, Offset, Limit, undefined, StartId) of
        {ok, Children, _, _} ->
            {ok, lists:map(fun({ChildGuid, _ChildName}) ->
                gs_protocol:gri_to_string(#gri{
                    type = op_file,
                    id = ChildGuid,
                    aspect = instance,
                    scope = private
                })
            end, Children)};
        {error, Errno} ->
            ?ERROR_POSIX(Errno)
    end.


%% @private
-spec register_file_upload(aai:auth(), gs_protocol:rpc_args()) ->
    gs_protocol:rpc_result().
register_file_upload(?USER(UserId, SessionId), Data) ->
    SanitizedData = op_sanitizer:sanitize_data(Data, #{
        required => #{<<"guid">> => {binary, non_empty}}
    }),
    FileGuid = maps:get(<<"guid">>, SanitizedData),

    case lfm:stat(SessionId, {guid, FileGuid}) of
        {ok, #file_attr{type = ?REGULAR_FILE_TYPE, size = 0, owner_id = UserId}} ->
            ok = file_upload_manager:register_upload(UserId, FileGuid),
            {ok, #{}};
        {ok, _} ->
            ?ERROR_BAD_DATA(<<"guid">>);
        {error, Errno} ->
            ?ERROR_POSIX(Errno)
    end.


%% @private
-spec deregister_file_upload(aai:auth(), gs_protocol:rpc_args()) ->
    gs_protocol:rpc_result().
deregister_file_upload(?USER(UserId), Data) ->
    SanitizedData = op_sanitizer:sanitize_data(Data, #{
        required => #{<<"guid">> => {binary, non_empty}}
    }),
    FileGuid = maps:get(<<"guid">>, SanitizedData),
    file_upload_manager:deregister_upload(UserId, FileGuid),
    {ok, #{}}.


%% @private
-spec get_file_download_url(aai:auth(), gs_protocol:rpc_args()) ->
    gs_protocol:rpc_result().
get_file_download_url(?USER(_UserId, SessId) = Auth, Data) ->
    SanitizedData = op_sanitizer:sanitize_data(Data, #{
        required => #{<<"guid">> => {binary, non_empty}}
    }),
    FileGuid = maps:get(<<"guid">>, SanitizedData),
    assert_space_membership_and_local_support(Auth, FileGuid),

    case page_file_download:get_file_download_url(SessId, FileGuid) of
        {ok, URL} ->
            {ok, #{<<"fileUrl">> => URL}};
        ?ERROR_FORBIDDEN ->
            ?ERROR_FORBIDDEN;
        {error, Errno} ->
            ?debug("Cannot resolve file download url for file ~p - ~p", [
                FileGuid, Errno
            ]),
            ?ERROR_POSIX(Errno)
    end.


%% @private
-spec move(aai:auth(), gs_protocol:rpc_args()) -> gs_protocol:rpc_result().
move(?USER(_UserId, SessionId) = Auth, Data) ->
    SanitizedData = op_sanitizer:sanitize_data(Data, #{
        required => #{
            <<"guid">> => {binary, non_empty},
            <<"targetParentGuid">> => {binary, non_empty},
            <<"targetName">> => {binary, non_empty}
        }
    }),
    FileGuid = maps:get(<<"guid">>, SanitizedData),
    TargetParentGuid = maps:get(<<"targetParentGuid">>, SanitizedData),
    TargetName = maps:get(<<"targetName">>, SanitizedData),

    assert_space_membership_and_local_support(Auth, FileGuid),
    assert_space_membership_and_local_support(Auth, TargetParentGuid),

    case lfm:mv(SessionId, {guid, FileGuid}, {guid, TargetParentGuid}, TargetName) of
        {ok, NewGuid} ->
            {ok, #{<<"id">> => gs_protocol:gri_to_string(#gri{
                type = op_file,
                id = NewGuid,
                aspect = instance,
                scope = private
            })}};
        {error, Errno} ->
            ?ERROR_POSIX(Errno)
    end.


%% @private
-spec copy(aai:auth(), gs_protocol:rpc_args()) -> gs_protocol:rpc_result().
copy(?USER(_UserId, SessionId) = Auth, Data) ->
    SanitizedData = op_sanitizer:sanitize_data(Data, #{
        required => #{
            <<"guid">> => {binary, non_empty},
            <<"targetParentGuid">> => {binary, non_empty},
            <<"targetName">> => {binary, non_empty}
        }
    }),
    FileGuid = maps:get(<<"guid">>, SanitizedData),
    TargetParentGuid = maps:get(<<"targetParentGuid">>, SanitizedData),
    TargetName = maps:get(<<"targetName">>, SanitizedData),

    assert_space_membership_and_local_support(Auth, FileGuid),
    assert_space_membership_and_local_support(Auth, TargetParentGuid),

    case lfm:cp(SessionId, {guid, FileGuid}, {guid, TargetParentGuid}, TargetName) of
        {ok, NewGuid} ->
            {ok, #{<<"id">> => gs_protocol:gri_to_string(#gri{
                type = op_file,
                id = NewGuid,
                aspect = instance,
                scope = private
            })}};
        {error, Errno} ->
            ?ERROR_POSIX(Errno)
    end.


%% @private
-spec assert_space_membership_and_local_support(aai:auth(), file_id:file_guid()) ->
    ok | no_return().
assert_space_membership_and_local_support(Auth, Guid) ->
    SpaceId = file_id:guid_to_space_id(Guid),
    case op_logic_utils:is_eff_space_member(Auth, SpaceId) of
        true ->
            op_logic_utils:assert_space_supported_locally(SpaceId);
        false ->
            throw(?ERROR_UNAUTHORIZED)
    end.
