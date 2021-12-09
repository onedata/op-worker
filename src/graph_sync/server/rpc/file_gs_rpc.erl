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
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([
    move/2, copy/2,

    register_file_upload/2, deregister_file_upload/2
]).


%%%===================================================================
%%% API
%%%===================================================================


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

    {ok, NewGuid} = ?lfm_check(lfm:mv(
        SessionId, ?FILE_REF(FileGuid), ?FILE_REF(TargetParentGuid), TargetName
    )),
    {ok, #{<<"id">> => gri:serialize(#gri{
        type = op_file, id = NewGuid,
        aspect = instance, scope = private
    })}}.


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

    {ok, NewGuid} = ?lfm_check(lfm:cp(
        SessionId, ?FILE_REF(FileGuid), ?FILE_REF(TargetParentGuid), TargetName
    )),
    {ok, #{<<"id">> => gri:serialize(#gri{
        type = op_file, id = NewGuid,
        aspect = instance, scope = private
    })}}.


-spec register_file_upload(aai:auth(), gs_protocol:rpc_args()) ->
    gs_protocol:rpc_result().
register_file_upload(?USER(UserId, SessionId), Data) ->
    SanitizedData = middleware_sanitizer:sanitize_data(Data, #{
        required => #{<<"guid">> => {binary, non_empty}}
    }),
    FileGuid = maps:get(<<"guid">>, SanitizedData),

    case ?lfm_check(lfm:stat(SessionId, ?FILE_REF(FileGuid))) of
        {ok, #file_attr{type = ?DIRECTORY_TYPE}} ->
            ?ERROR_BAD_DATA(<<"guid">>, <<"not a regular file">>);
        {ok, #file_attr{type = ?REGULAR_FILE_TYPE, size = 0, owner_id = UserId}} ->
            ok = file_upload_manager:register_upload(UserId, FileGuid),
            {ok, #{}};
        {ok, #file_attr{type = ?REGULAR_FILE_TYPE, size = 0}} ->
            ?ERROR_BAD_DATA(<<"guid">>, <<"file is not owned by user">>);
        {ok, #file_attr{type = ?REGULAR_FILE_TYPE}} ->
            ?ERROR_BAD_DATA(<<"guid">>, <<"file is not empty">>)
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
            throw(?ERROR_FORBIDDEN)
    end.
