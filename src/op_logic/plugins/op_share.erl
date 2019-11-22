%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles op logic operations (create, get, update, delete)
%%% corresponding to directory sharing.
%%% @end
%%%-------------------------------------------------------------------
-module(op_share).
-author("Bartosz Walkowicz").

-behaviour(op_logic_behaviour).

-include("op_logic.hrl").
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
%% {@link op_logic_behaviour} callback operation_supported/3.
%% @end
%%--------------------------------------------------------------------
-spec operation_supported(op_logic:operation(), gri:aspect(),
    op_logic:scope()) -> boolean().
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
%% {@link op_logic_behaviour} callback data_spec/1.
%% @end
%%--------------------------------------------------------------------
-spec data_spec(op_logic:req()) -> undefined | op_sanitizer:data_spec().
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
%% {@link op_logic_behaviour} callback fetch_entity/1.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(op_logic:req()) ->
    {ok, op_logic:versioned_entity()} | errors:error().
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
%% {@link op_logic_behaviour} callback authorize/2.
%%
%% Checks only membership in space. Share management privileges
%% are checked later by oz.
%% @end
%%--------------------------------------------------------------------
-spec authorize(op_logic:req(), op_logic:entity()) -> boolean().
authorize(#op_req{auth = ?NOBODY}, _) ->
    false;

authorize(#op_req{operation = create, auth = Auth, gri = #gri{
    id = DirGuid,
    aspect = shared_dir
}}, _) ->
    SpaceId = file_id:guid_to_space_id(DirGuid),
    op_logic_utils:is_eff_space_member(Auth, SpaceId);

authorize(#op_req{operation = get, auth = Auth, gri = #gri{aspect = instance}},
    #od_share{space = SpaceId}
) ->
    op_logic_utils:is_eff_space_member(Auth, SpaceId);

authorize(#op_req{operation = get, auth = Auth, gri = #gri{
    id = DirGuid,
    aspect = shared_dir
}}, _) ->
    SpaceId = file_id:guid_to_space_id(DirGuid),
    op_logic_utils:is_eff_space_member(Auth, SpaceId);

authorize(#op_req{operation = update, auth = Auth, gri = #gri{aspect = instance}},
    #od_share{space = SpaceId}
) ->
    op_logic_utils:is_eff_space_member(Auth, SpaceId);

authorize(#op_req{operation = update, auth = Auth, gri = #gri{
    id = DirGuid,
    aspect = shared_dir
}}, _) ->
    SpaceId = file_id:guid_to_space_id(DirGuid),
    op_logic_utils:is_eff_space_member(Auth, SpaceId);

authorize(#op_req{operation = delete, auth = Auth, gri = #gri{aspect = instance}},
    #od_share{space = SpaceId}
) ->
    op_logic_utils:is_eff_space_member(Auth, SpaceId);

authorize(#op_req{operation = delete, auth = Auth, gri = #gri{
    id = DirGuid,
    aspect = shared_dir
}}, _) ->
    SpaceId = file_id:guid_to_space_id(DirGuid),
    op_logic_utils:is_eff_space_member(Auth, SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback validate/2.
%% @end
%%--------------------------------------------------------------------
-spec validate(op_logic:req(), op_logic:entity()) -> ok | no_return().
validate(#op_req{operation = create, gri = #gri{id = Guid, aspect = shared_dir}}, _) ->
    SpaceId = file_id:guid_to_space_id(Guid),
    op_logic_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = get, gri = #gri{aspect = instance}}, #od_share{
    space = SpaceId
}) ->
    op_logic_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = get, gri = #gri{id = DirGuid, aspect = shared_dir}}, _) ->
    SpaceId = file_id:guid_to_space_id(DirGuid),
    op_logic_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = update, gri = #gri{aspect = instance}}, #od_share{
    space = SpaceId
}) ->
    op_logic_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = update, gri = #gri{id = DirGuid, aspect = shared_dir}}, _) ->
    SpaceId = file_id:guid_to_space_id(DirGuid),
    op_logic_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = delete, gri = #gri{aspect = instance}}, #od_share{
    space = SpaceId
}) ->
    op_logic_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = delete, gri = #gri{id = DirGuid, aspect = shared_dir}}, _) ->
    SpaceId = file_id:guid_to_space_id(DirGuid),
    op_logic_utils:assert_space_supported_locally(SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(op_logic:req()) -> op_logic:create_result().
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
%% {@link op_logic_behaviour} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(op_logic:req(), op_logic:entity()) -> op_logic:get_result().
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
    root_file = RootFile,
    name = ShareName,
    public_url = SharePublicUrl,
    handle = Handle
}) ->
    {ok, ObjectId} = file_id:guid_to_objectid(RootFile),

    {ok, #{
        <<"shareId">> => ShareId,
        <<"name">> => ShareName,
        <<"publicUrl">> => SharePublicUrl,
        <<"rootFileId">> => ObjectId,
        <<"spaceId">> => SpaceId,
        <<"handleId">> => utils:ensure_defined(Handle, undefined, null)
    }}.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback update/1.
%% @end
%%--------------------------------------------------------------------
-spec update(op_logic:req()) -> op_logic:update_result().
update(#op_req{auth = Auth, gri = #gri{id = DirGuid, aspect = shared_dir}} = Req) ->
    ShareId = resolve_share_id(Auth, DirGuid),
    NewName = maps:get(<<"name">>, Req#op_req.data),
    share_logic:update_name(Auth#auth.session_id, ShareId, NewName);

update(#op_req{auth = Auth, gri = #gri{id = ShareId, aspect = instance}} = Req) ->
    NewName = maps:get(<<"name">>, Req#op_req.data),
    share_logic:update_name(Auth#auth.session_id, ShareId, NewName).


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(op_logic:req()) -> op_logic:delete_result().
delete(#op_req{auth = Auth, gri = #gri{id = DirGuid, aspect = shared_dir}}) ->
    ShareId = resolve_share_id(Auth, DirGuid),
    case lfm:remove_share(Auth#auth.session_id, ShareId) of
        ok -> ok;
        {error, Errno} -> ?ERROR_POSIX(Errno)
    end;

delete(#op_req{auth = Auth, gri = #gri{id = ShareId, aspect = instance}}) ->
    case lfm:remove_share(Auth#auth.session_id, ShareId) of
        ok -> ok;
        {error, Errno} -> ?ERROR_POSIX(Errno)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec fetch_share(aai:auth(), od_share:id()) ->
    {ok, {#od_share{}, op_logic:revision()}} | ?ERROR_NOT_FOUND.
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
