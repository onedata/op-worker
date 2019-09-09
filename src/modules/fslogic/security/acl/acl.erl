%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Utility functions for access control list management.
%%% @end
%%%--------------------------------------------------------------------
-module(acl).
-author("Bartosz Walkowicz").

-include("modules/fslogic/metadata.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/errors.hrl").

-type acl() :: [ace:ace()].

-export_type([acl/0]).

%% API
-export([
    get/1, exists/1,
    assert_permitted/4,

    from_json/2, to_json/2
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec get(file_ctx:ctx()) -> undefined | acl().
get(FileCtx) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    case custom_metadata:get_xattr_metadata(FileUuid, ?ACL_KEY, false) of
        {ok, Val} ->
            from_json(Val, cdmi);
        {error, not_found} ->
            undefined
    end.


-spec exists(file_id:file_guid() | file_ctx:ctx()) -> boolean().
exists(FileGuid) when is_binary(FileGuid) ->
    FileUuid = file_id:guid_to_uuid(FileGuid),
    custom_metadata:exists_xattr_metadata(FileUuid, ?ACL_XATTR_NAME);
exists(FileCtx) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    custom_metadata:exists_xattr_metadata(FileUuid, ?ACL_XATTR_NAME).


-spec assert_permitted(acl(), od_user:doc(), ace:bitmask(), file_ctx:ctx()) ->
    ok | no_return().
assert_permitted(_Acl, _User, ?no_flags_mask, _FileCtx) ->
    ok;
assert_permitted([], _User, _Operations, _FileCtx) ->
    throw(?EACCES);
assert_permitted([Ace | Rest], User, Operations, FileCtx) ->
    case ace:is_applicable(User, FileCtx, Ace) of
        {true, FileCtx2} ->
            case ace:check_against(Operations, Ace) of
                allowed ->
                    ok;
                {inconclusive, LeftoverOperations} ->
                    assert_permitted(Rest, User, LeftoverOperations, FileCtx2);
                denied ->
                    throw(?EACCES)
            end;
        {false, FileCtx2} ->
            assert_permitted(Rest, User, Operations, FileCtx2)
    end.


-spec from_json([map()], Format :: gui | cdmi) -> acl() | no_return().
from_json(JsonAcl, Format) ->
    try
        [ace:from_json(JsonAce, Format) || JsonAce <- JsonAcl]
    catch Type:Reason ->
        ?debug_stacktrace("Failed to translate json(~p) to acl due to: ~p:~p", [
            Format, Type, Reason
        ]),
        throw({error, ?EINVAL})
    end.


-spec to_json(acl(), Format :: gui | cdmi) -> [map()] | no_return().
to_json(Acl, Format) ->
    try
        [ace:to_json(Ace, Format) || Ace <- Acl]
    catch Type:Reason ->
        ?debug_stacktrace("Failed to convert acl to json(~p) due to: ~p:~p", [
            Format, Type, Reason
        ]),
        throw({error, ?EINVAL})
    end.
