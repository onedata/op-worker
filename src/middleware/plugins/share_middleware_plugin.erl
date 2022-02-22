%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019-2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles middleware operations (create, get, update, delete)
%%% corresponding to shares.
%%% @end
%%%-------------------------------------------------------------------
-module(share_middleware_plugin).
-author("Bartosz Walkowicz").

-behaviour(middleware_router).
-behaviour(middleware_handler).

-include("middleware/middleware.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl").

%% middleware_router callbacks
-export([resolve_handler/3]).

%% middleware_handler callbacks
-export([data_spec/1, fetch_entity/1, authorize/2, validate/2]).
-export([create/1, get/2, update/1, delete/1]).


%%%===================================================================
%%% middleware_router callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_router} callback resolve_handler/3.
%% @end
%%--------------------------------------------------------------------
-spec resolve_handler(middleware:operation(), gri:aspect(), middleware:scope()) ->
    module() | no_return().
resolve_handler(create, instance, private) -> ?MODULE;

resolve_handler(get, instance, private) -> ?MODULE;
resolve_handler(get, instance, public) -> ?MODULE;

resolve_handler(update, instance, private) -> ?MODULE;

resolve_handler(delete, instance, private) -> ?MODULE;

resolve_handler(_, _, _) -> throw(?ERROR_NOT_SUPPORTED).


%%%===================================================================
%%% middleware_handler callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback data_spec/1.
%% @end
%%--------------------------------------------------------------------
-spec data_spec(middleware:req()) -> undefined | middleware_sanitizer:data_spec().
data_spec(#op_req{operation = create, gri = #gri{aspect = instance}}) -> #{
    required => #{
        <<"name">> => {binary, non_empty}
    },
    at_least_one => #{
        <<"fileId">> => {binary, fun(ObjectId) ->
            {true, middleware_utils:decode_object_id(ObjectId, <<"fileId">>)}
        end},
        <<"rootFileId">> => {binary, fun(ObjectId) ->
            {true, middleware_utils:decode_object_id(ObjectId, <<"rootFileId">>)}
        end}
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
%% {@link middleware_handler} callback fetch_entity/1.
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
%% {@link middleware_handler} callback authorize/2.
%%
%% Checks only membership in space. Share management privileges
%% are checked later by oz.
%% @end
%%--------------------------------------------------------------------
-spec authorize(middleware:req(), middleware:entity()) -> boolean().
authorize(#op_req{operation = create, auth = Auth, gri = #gri{aspect = instance}, data = Data}, _) ->
    RootFileGuid = get_root_file_guid(Data),
    SpaceId = file_id:guid_to_space_id(RootFileGuid),
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
%% {@link middleware_handler} callback validate/2.
%% @end
%%--------------------------------------------------------------------
-spec validate(middleware:req(), middleware:entity()) -> ok | no_return().
validate(#op_req{operation = create, gri = #gri{aspect = instance}, data = Data}, _) ->
    RootFileGuid = get_root_file_guid(Data),
    SpaceId = file_id:guid_to_space_id(RootFileGuid),
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
%% {@link middleware_handler} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(middleware:req()) -> middleware:create_result().
create(#op_req{auth = Auth, data = Data, gri = #gri{aspect = instance} = GRI}) ->
    SessionId = Auth#auth.session_id,
    FileRef = ?FILE_REF(get_root_file_guid(Data)),

    Name = maps:get(<<"name">>, Data),
    Description = maps:get(<<"description">>, Data, <<"">>),

    ShareId = mi_shares:create(SessionId, FileRef, Name, Description),

    case share_logic:get(SessionId, ShareId) of
        {ok, #document{value = ShareRec}} ->
            Share = share_to_json(ShareId, ShareRec),
            {ok, resource, {GRI#gri{id = ShareId}, Share}};
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(middleware:req(), middleware:entity()) -> middleware:get_result().
get(#op_req{gri = #gri{id = ShareId, aspect = instance}}, Share) ->
    {ok, share_to_json(ShareId, Share)}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback update/1.
%% @end
%%--------------------------------------------------------------------
-spec update(middleware:req()) -> middleware:update_result().
update(#op_req{auth = Auth, gri = #gri{id = ShareId, aspect = instance}} = Req) ->
    share_logic:update(Auth#auth.session_id, ShareId, Req#op_req.data).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(middleware:req()) -> middleware:delete_result().
delete(#op_req{auth = Auth, gri = #gri{id = ShareId, aspect = instance}}) ->
    mi_shares:remove(Auth#auth.session_id, ShareId).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_root_file_guid(middleware:data()) -> file_id:file_guid().
get_root_file_guid(Data) ->
    case maps:get(<<"rootFileId">>, Data, undefined) of
        undefined -> maps:get(<<"fileId">>, Data);
        RootFileGuid -> RootFileGuid
    end.


%% @private
-spec share_to_json(od_share:id(), od_share:record()) -> map().
share_to_json(ShareId, #od_share{
    space = SpaceId,
    name = ShareName,
    description = Description,
    root_file = RootFileGuid,
    public_url = PublicUrl,
    public_rest_url = PublicRestUrl,
    file_type = FileType,
    handle = Handle
}) ->
    #{
        <<"shareId">> => ShareId,
        <<"spaceId">> => utils:undefined_to_null(SpaceId),
        <<"name">> => ShareName,
        <<"description">> => Description,
        <<"publicUrl">> => PublicUrl,
        <<"publicRestUrl">> => PublicRestUrl,
        <<"rootFileId">> => RootFileGuid,
        <<"rootFileType">> => case FileType of
            file -> <<"REG">>;
            dir -> <<"DIR">>
        end,
        <<"handleId">> => utils:undefined_to_null(Handle)
    }.
