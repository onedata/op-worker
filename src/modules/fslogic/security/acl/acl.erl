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

-define(convert(__Conversion),
    try
        __Conversion
    catch __T:__R  ->
        ?debug_stacktrace("Failed to convert acl due to: ~p:~p", [__T, __R]),
        throw({error, ?EINVAL})
    end
).

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


-spec exists(file_ctx:ctx()) -> boolean().
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
            case ace:check_permission(Operations, Ace) of
                ok ->
                    ok;
                LeftoverOperations ->
                    assert_permitted(Rest, User, LeftoverOperations, FileCtx2)
            end;
        {false, FileCtx2} ->
            assert_permitted(Rest, User, Operations, FileCtx2)
    end.


-spec from_json([map()], Format :: gui | cdmi) -> acl() | no_return().
from_json(JsonAcl, Format) ->
    ?convert([ace:from_json(JsonAce, Format) || JsonAce <- JsonAcl]).


-spec to_json(acl(), Format :: gui | cdmi) -> [map()] | no_return().
to_json(Acl, Format) ->
    ?convert([ace:to_json(Ace, Format) || Ace <- Acl]).
