%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles middleware operations (create, get, update, delete)
%%% corresponding to directory sharing.
%%% @end
%%%-------------------------------------------------------------------
-module(share_middleware).
-author("Bartosz Walkowicz").

-behaviour(middleware_plugin).

-include("middleware/middleware.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl").

-export([
    operation_supported/3,
    data_spec/1,
    fetch_entity/1,
    authorize/2,
    validate/2
]).
-export([create/1, get/2, update/1, delete/1]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback operation_supported/3.
%% @end
%%--------------------------------------------------------------------
-spec operation_supported(middleware:operation(), gri:aspect(),
    middleware:scope()) -> boolean().
operation_supported(create, shared_dir, private) -> true;

operation_supported(get, instance, private) -> true;
operation_supported(get, shared_dir, private) -> true;

operation_supported(update, instance, private) -> true;
operation_supported(update, shared_dir, private) -> true;

operation_supported(delete, instance, private) -> true;
operation_supported(delete, shared_dir, private) -> true;

operation_supported(_, _, _) -> false.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback data_spec/1.
%% @end
%%--------------------------------------------------------------------
-spec data_spec(middleware:req()) -> undefined | middleware_sanitizer:data_spec().
data_spec(#op_req{operation = create, gri = #gri{aspect = shared_dir}}) -> #{
    required => #{<<"name">> => {binary, non_empty}}
};

data_spec(#op_req{operation = get, gri = #gri{aspect = instance}}) ->
    undefined;

data_spec(#op_req{operation = get, gri = #gri{aspect = shared_dir}}) ->
    undefined;

data_spec(#op_req{operation = update, gri = #gri{aspect = instance}}) ->
    #{required => #{<<"name">> => {binary, non_empty}}};

data_spec(#op_req{operation = update, gri = #gri{aspect = shared_dir}}) ->
    #{required => #{<<"name">> => {binary, non_empty}}};

data_spec(#op_req{operation = delete, gri = #gri{aspect = instance}}) ->
    undefined;

data_spec(#op_req{operation = delete, gri = #gri{aspect = shared_dir}}) ->
    undefined.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback fetch_entity/1.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(middleware:req()) ->
    {ok, middleware:versioned_entity()} | errors:error().
fetch_entity(#op_req{operation = create, gri = #gri{aspect = shared_dir}}) ->
    {ok, {undefined, 1}};

fetch_entity(#op_req{operation = get, auth = Auth, gri = #gri{
    id = ShareId,
    aspect = instance
}}) ->
    fetch_share(Auth, ShareId);

fetch_entity(#op_req{operation = get, gri = #gri{aspect = shared_dir}}) ->
    {ok, {undefined, 1}};

fetch_entity(#op_req{operation = update, auth = Auth, gri = #gri{
    id = ShareId,
    aspect = instance
}}) ->
    fetch_share(Auth, ShareId);

fetch_entity(#op_req{operation = update, gri = #gri{aspect = shared_dir}}) ->
    {ok, {undefined, 1}};

fetch_entity(#op_req{operation = delete, auth = Auth, gri = #gri{
    id = ShareId,
    aspect = instance
}}) ->
    fetch_share(Auth, ShareId);

fetch_entity(#op_req{operation = delete, gri = #gri{aspect = shared_dir}}) ->
    {ok, {undefined, 1}}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback authorize/2.
%%
%% Checks only membership in space. Share management privileges
%% are checked later by oz.
%% @end
%%--------------------------------------------------------------------
-spec authorize(middleware:req(), middleware:entity()) -> boolean().
authorize(#op_req{auth = ?NOBODY}, _) ->
    false;

authorize(#op_req{operation = create, auth = Auth, gri = #gri{
    id = DirGuid,
    aspect = shared_dir
}}, _) ->
    SpaceId = file_id:guid_to_space_id(DirGuid),
    middleware_utils:is_eff_space_member(Auth, SpaceId);

authorize(#op_req{operation = get, auth = Auth, gri = #gri{aspect = instance}},
    #od_share{space = SpaceId}
) ->
    middleware_utils:is_eff_space_member(Auth, SpaceId);

authorize(#op_req{operation = get, auth = Auth, gri = #gri{
    id = DirGuid,
    aspect = shared_dir
}}, _) ->
    SpaceId = file_id:guid_to_space_id(DirGuid),
    middleware_utils:is_eff_space_member(Auth, SpaceId);

authorize(#op_req{operation = update, auth = Auth, gri = #gri{aspect = instance}},
    #od_share{space = SpaceId}
) ->
    middleware_utils:is_eff_space_member(Auth, SpaceId);

authorize(#op_req{operation = update, auth = Auth, gri = #gri{
    id = DirGuid,
    aspect = shared_dir
}}, _) ->
    SpaceId = file_id:guid_to_space_id(DirGuid),
    middleware_utils:is_eff_space_member(Auth, SpaceId);

authorize(#op_req{operation = delete, auth = Auth, gri = #gri{aspect = instance}},
    #od_share{space = SpaceId}
) ->
    middleware_utils:is_eff_space_member(Auth, SpaceId);

authorize(#op_req{operation = delete, auth = Auth, gri = #gri{
    id = DirGuid,
    aspect = shared_dir
}}, _) ->
    SpaceId = file_id:guid_to_space_id(DirGuid),
    middleware_utils:is_eff_space_member(Auth, SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback validate/2.
%% @end
%%--------------------------------------------------------------------
-spec validate(middleware:req(), middleware:entity()) -> ok | no_return().
validate(#op_req{operation = create, gri = #gri{id = Guid, aspect = shared_dir}}, _) ->
    SpaceId = file_id:guid_to_space_id(Guid),
    middleware_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = get, gri = #gri{aspect = instance}}, #od_share{
    space = SpaceId
}) ->
    middleware_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = get, gri = #gri{id = DirGuid, aspect = shared_dir}}, _) ->
    SpaceId = file_id:guid_to_space_id(DirGuid),
    middleware_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = update, gri = #gri{aspect = instance}}, #od_share{
    space = SpaceId
}) ->
    middleware_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = update, gri = #gri{id = DirGuid, aspect = shared_dir}}, _) ->
    SpaceId = file_id:guid_to_space_id(DirGuid),
    middleware_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = delete, gri = #gri{aspect = instance}}, #od_share{
    space = SpaceId
}) ->
    middleware_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = delete, gri = #gri{id = DirGuid, aspect = shared_dir}}, _) ->
    SpaceId = file_id:guid_to_space_id(DirGuid),
    middleware_utils:assert_space_supported_locally(SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(middleware:req()) -> middleware:create_result().
create(#op_req{auth = Auth, gri = #gri{id = DirGuid, aspect = shared_dir}} = Req) ->
    Name = maps:get(<<"name">>, Req#op_req.data),
    case lfm:create_share(Auth#auth.session_id, {guid, DirGuid}, Name) of
        {ok, {ShareId, _ShareGuid}} ->
            {ok, value, ShareId};
        {error, Errno} ->
            ?ERROR_POSIX(Errno)
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(middleware:req(), middleware:entity()) -> middleware:get_result().
get(#op_req{auth = Auth, gri = #gri{id = DirGuid, aspect = shared_dir} = GRI} = Req, _) ->
    ShareId = resolve_share_id(Auth, DirGuid),
    case fetch_share(Auth, ShareId) of
        {ok, {Share, _}} ->
            get(Req#op_req{gri = GRI#gri{id = ShareId, aspect = instance}}, Share);
        {error, _} = Error ->
            Error
    end;

get(#op_req{gri = #gri{id = ShareId, aspect = instance}}, #od_share{
    space = SpaceId,
    root_file = RootFileGuid,
    name = ShareName,
    public_url = SharePublicUrl,
    handle = Handle
}) ->
    {ok, #{
        <<"shareId">> => ShareId,
        <<"name">> => ShareName,
        <<"publicUrl">> => SharePublicUrl,
        <<"rootFileId">> => RootFileGuid,
        <<"spaceId">> => SpaceId,
        <<"handleId">> => utils:ensure_defined(Handle, undefined, null)
    }}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback update/1.
%% @end
%%--------------------------------------------------------------------
-spec update(middleware:req()) -> middleware:update_result().
update(#op_req{auth = Auth, gri = #gri{id = DirGuid, aspect = shared_dir}} = Req) ->
    ShareId = resolve_share_id(Auth, DirGuid),
    NewName = maps:get(<<"name">>, Req#op_req.data),
    share_logic:update_name(Auth#auth.session_id, ShareId, NewName);

update(#op_req{auth = Auth, gri = #gri{id = ShareId, aspect = instance}} = Req) ->
    NewName = maps:get(<<"name">>, Req#op_req.data),
    share_logic:update_name(Auth#auth.session_id, ShareId, NewName).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(middleware:req()) -> middleware:delete_result().
delete(#op_req{auth = Auth, gri = #gri{id = DirGuid, aspect = shared_dir}}) ->
    ShareId = resolve_share_id(Auth, DirGuid),
    ?check(lfm:remove_share(Auth#auth.session_id, ShareId));

delete(#op_req{auth = Auth, gri = #gri{id = ShareId, aspect = instance}}) ->
    ?check(lfm:remove_share(Auth#auth.session_id, ShareId)).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec fetch_share(aai:auth(), od_share:id()) ->
    {ok, {#od_share{}, middleware:revision()}} | ?ERROR_NOT_FOUND.
fetch_share(?USER(_UserId, SessionId), ShareId) ->
    case share_logic:get(SessionId, ShareId) of
        {ok, #document{value = Share}} ->
            {ok, {Share, 1}};
        {error, _} = Error ->
            Error
    end.


%% @private
-spec resolve_share_id(aai:auth(), file_id:file_guid()) ->
    od_share:id() | ?ERROR_NOT_FOUND.
resolve_share_id(?USER(_UserId, SessionId), DirGuid) ->
    case lfm:stat(SessionId, {guid, DirGuid}) of
        {ok, #file_attr{shares = [ShareId]}} ->
            ShareId;
        _ ->
            throw(?ERROR_NOT_FOUND)
    end.
