%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles middleware operations (create, get, update, delete)
%%% corresponding to shares.
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
operation_supported(create, instance, private) -> true;

operation_supported(get, instance, private) -> true;
operation_supported(get, instance, public) -> true;

operation_supported(update, instance, private) -> true;

operation_supported(delete, instance, private) -> true;

operation_supported(_, _, _) -> false.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback data_spec/1.
%% @end
%%--------------------------------------------------------------------
-spec data_spec(middleware:req()) -> undefined | middleware_sanitizer:data_spec().
data_spec(#op_req{operation = create, gri = #gri{aspect = instance}}) -> #{
    required => #{
        <<"name">> => {binary, non_empty},
        <<"fileId">> => {binary,
            fun(ObjectId) -> {true, middleware_utils:decode_object_id(ObjectId, <<"fileId">>)} end}
    },
    optional => #{
        <<"description">> => {binary, any}
    }
};

data_spec(#op_req{operation = get, gri = #gri{aspect = instance}}) ->
    undefined;

data_spec(#op_req{operation = update, gri = #gri{aspect = instance}}) -> #{
    at_least_one => #{
        <<"name">> => {binary, non_empty},
        <<"description">> => {binary, any}
    }
};

data_spec(#op_req{operation = delete, gri = #gri{aspect = instance}}) ->
    undefined.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback fetch_entity/1.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(middleware:req()) ->
    {ok, middleware:versioned_entity()} | errors:error().
fetch_entity(#op_req{operation = get, auth = Auth, gri = #gri{
    id = ShareId,
    aspect = instance,
    scope = public
}}) ->
    case share_logic:get_public_data(Auth#auth.session_id, ShareId) of
        {ok, #document{value = Share}} ->
            {ok, {Share, 1}};
        {error, _} = Error ->
            Error
    end;

fetch_entity(#op_req{auth = ?NOBODY}) ->
    ?ERROR_UNAUTHORIZED;

fetch_entity(#op_req{operation = Op, auth = ?USER(_UserId, SessionId), gri = #gri{
    id = ShareId,
    aspect = instance,
    scope = private
}}) when
    Op =:= get;
    Op =:= update;
    Op =:= delete
->
    case share_logic:get(SessionId, ShareId) of
        {ok, #document{value = Share}} ->
            {ok, {Share, 1}};
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback authorize/2.
%%
%% Checks only membership in space. Share management privileges
%% are checked later by oz.
%% @end
%%--------------------------------------------------------------------
-spec authorize(middleware:req(), middleware:entity()) -> boolean().
authorize(#op_req{operation = create, auth = Auth, gri = #gri{aspect = instance}, data = #{
    <<"fileId">> := FileGuid
}}, _) ->
    SpaceId = file_id:guid_to_space_id(FileGuid),
    middleware_utils:is_eff_space_member(Auth, SpaceId);

authorize(#op_req{operation = get, gri = #gri{aspect = instance, scope = public}}, _) ->
    true;

authorize(#op_req{operation = Op, auth = Auth, gri = #gri{aspect = instance}},
    #od_share{space = SpaceId}
) when
    Op =:= get;
    Op =:= update;
    Op =:= delete
->
    middleware_utils:is_eff_space_member(Auth, SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback validate/2.
%% @end
%%--------------------------------------------------------------------
-spec validate(middleware:req(), middleware:entity()) -> ok | no_return().
validate(#op_req{operation = create, gri = #gri{aspect = instance}, data = #{
    <<"fileId">> := FileGuid
}}, _) ->
    SpaceId = file_id:guid_to_space_id(FileGuid),
    middleware_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = get, gri = #gri{aspect = instance, scope = public}}, _) ->
    ok;

validate(#op_req{operation = Op, gri = #gri{aspect = instance}},
    #od_share{space = SpaceId}
) when
    Op =:= get;
    Op =:= update;
    Op =:= delete
->
    middleware_utils:assert_space_supported_locally(SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(middleware:req()) -> middleware:create_result().
create(#op_req{auth = Auth, data = Data, gri = #gri{aspect = instance} = GRI} = Req) ->
    SessionId = Auth#auth.session_id,
    FileKey = {guid, maps:get(<<"fileId">>, Req#op_req.data)},

    Name = maps:get(<<"name">>, Data),
    Description = maps:get(<<"description">>, Data, <<"">>),

    {ok, ShareId} = ?check(lfm:create_share(SessionId, FileKey, Name, Description)),

    case share_logic:get(SessionId, ShareId) of
        {ok, #document{value = ShareRec}} ->
            Share = share_to_json(ShareId, ShareRec),
            {ok, resource, {GRI#gri{id = ShareId}, Share}};
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(middleware:req(), middleware:entity()) -> middleware:get_result().
get(#op_req{gri = #gri{id = ShareId, aspect = instance}}, Share) ->
    {ok, share_to_json(ShareId, Share)}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback update/1.
%% @end
%%--------------------------------------------------------------------
-spec update(middleware:req()) -> middleware:update_result().
update(#op_req{auth = Auth, gri = #gri{id = ShareId, aspect = instance}} = Req) ->
    share_logic:update(Auth#auth.session_id, ShareId, Req#op_req.data).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(middleware:req()) -> middleware:delete_result().
delete(#op_req{auth = Auth, gri = #gri{id = ShareId, aspect = instance}}) ->
    ?check(lfm:remove_share(Auth#auth.session_id, ShareId)).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec share_to_json(od_share:id(), od_share:record()) -> map().
share_to_json(ShareId, #od_share{
    space = SpaceId,
    name = ShareName,
    description = Description,
    root_file = RootFileGuid,
    public_url = SharePublicUrl,
    file_type = FileType,
    handle = Handle
}) ->
    #{
        <<"shareId">> => ShareId,
        <<"name">> => ShareName,
        <<"description">> => Description,
        <<"fileType">> => FileType,
        <<"publicUrl">> => SharePublicUrl,
        <<"rootFileId">> => RootFileGuid,
        <<"spaceId">> => utils:undefined_to_null(SpaceId),
        <<"handleId">> => utils:undefined_to_null(Handle)
    }.
