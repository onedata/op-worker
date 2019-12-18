%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles gs rpc concerning file entities.
%%% @end
%%%-------------------------------------------------------------------
-module(file_gs_rpc).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([
    ls/2,
    move/2, copy/2,

    register_file_upload/2, deregister_file_upload/2,
    get_file_download_url/2
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec ls(aai:auth(), gs_protocol:rpc_args()) -> gs_protocol:rpc_result().
ls(?USER(_UserId, SessionId) = Auth, Data) ->
    SanitizedData = middleware_sanitizer:sanitize_data(Data, #{
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
                gri:serialize(#gri{
                    type = op_file,
                    id = ChildGuid,
                    aspect = instance,
                    scope = private
                })
            end, Children)};
        {error, Errno} ->
            ?ERROR_POSIX(Errno)
    end.


-spec move(aai:auth(), gs_protocol:rpc_args()) -> gs_protocol:rpc_result().
move(?USER(_UserId, SessionId) = Auth, Data) ->
    SanitizedData = middleware_sanitizer:sanitize_data(Data, #{
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
            {ok, #{<<"id">> => gri:serialize(#gri{
                type = op_file,
                id = NewGuid,
                aspect = instance,
                scope = private
            })}};
        {error, Errno} ->
            ?ERROR_POSIX(Errno)
    end.


-spec copy(aai:auth(), gs_protocol:rpc_args()) -> gs_protocol:rpc_result().
copy(?USER(_UserId, SessionId) = Auth, Data) ->
    SanitizedData = middleware_sanitizer:sanitize_data(Data, #{
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
            {ok, #{<<"id">> => gri:serialize(#gri{
                type = op_file,
                id = NewGuid,
                aspect = instance,
                scope = private
            })}};
        {error, Errno} ->
            ?ERROR_POSIX(Errno)
    end.


-spec register_file_upload(aai:auth(), gs_protocol:rpc_args()) ->
    gs_protocol:rpc_result().
register_file_upload(?USER(UserId, SessionId), Data) ->
    SanitizedData = middleware_sanitizer:sanitize_data(Data, #{
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


-spec deregister_file_upload(aai:auth(), gs_protocol:rpc_args()) ->
    gs_protocol:rpc_result().
deregister_file_upload(?USER(UserId), Data) ->
    SanitizedData = middleware_sanitizer:sanitize_data(Data, #{
        required => #{<<"guid">> => {binary, non_empty}}
    }),
    FileGuid = maps:get(<<"guid">>, SanitizedData),
    file_upload_manager:deregister_upload(UserId, FileGuid),
    {ok, #{}}.


-spec get_file_download_url(aai:auth(), gs_protocol:rpc_args()) ->
    gs_protocol:rpc_result().
get_file_download_url(?USER(_UserId, SessId) = Auth, Data) ->
    SanitizedData = middleware_sanitizer:sanitize_data(Data, #{
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


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec assert_space_membership_and_local_support(aai:auth(), file_id:file_guid()) ->
    ok | no_return().
assert_space_membership_and_local_support(Auth, Guid) ->
    SpaceId = file_id:guid_to_space_id(Guid),
    case middleware_utils:is_eff_space_member(Auth, SpaceId) of
        true ->
            middleware_utils:assert_space_supported_locally(SpaceId);
        false ->
            throw(?ERROR_UNAUTHORIZED)
    end.
