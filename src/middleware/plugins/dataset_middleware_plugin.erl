%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles middleware operations (create, get, update, delete)
%%% corresponding to datasets.
%%% @end
%%%-------------------------------------------------------------------
-module(dataset_middleware_plugin).
-author("Bartosz Walkowicz").

-behaviour(middleware_router).
-behaviour(middleware_handler).

-include("middleware/middleware.hrl").
-include("modules/dataset/dataset.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/errors.hrl").

%% middleware_router callbacks
-export([resolve_handler/3]).

%% middleware_handler callbacks
-export([data_spec/1, fetch_entity/1, authorize/2, validate/2]).
-export([create/1, get/2, update/1, delete/1]).

% Util functions
-export([gather_listing_opts/1]).


-define(MAX_LIST_LIMIT, 1000).
-define(DEFAULT_LIST_LIMIT, 1000).
-define(ALL_PROTECTION_FLAGS, [?DATA_PROTECTION_BIN, ?METADATA_PROTECTION_BIN]).


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
resolve_handler(get, children, private) -> ?MODULE;
resolve_handler(get, children_details, private) -> ?MODULE;
resolve_handler(get, archives, private) -> ?MODULE;
resolve_handler(get, archives_details, private) -> ?MODULE;

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
        <<"rootFileId">> => {binary, fun(ObjectId) ->
            {true, middleware_utils:decode_object_id(ObjectId, <<"rootFileId">>)}
        end}
    },
    optional => #{
        <<"protectionFlags">> => {list_of_binaries, ?ALL_PROTECTION_FLAGS}
    }
};

data_spec(#op_req{operation = get, gri = #gri{aspect = instance}}) ->
    undefined;

data_spec(#op_req{operation = get, gri = #gri{aspect = Aspect}})
    when Aspect =:= children
    orelse Aspect =:= children_details
-> #{
    optional => #{
        <<"offset">> => {integer, any},
        <<"index">> => {binary, any},
        <<"token">> => {binary, non_empty},
        <<"limit">> => {integer, {between, 1, ?MAX_LIST_LIMIT}}
    }
};

data_spec(#op_req{operation = get, gri = #gri{aspect = Aspect}})
    when Aspect =:= archives
    orelse Aspect =:= archives_details
-> #{
    optional => #{
        <<"offset">> => {integer, any},
        <<"index">> => {binary, any},
        <<"token">> => {binary, non_empty},
        <<"limit">> => {integer, {between, 1, ?MAX_LIST_LIMIT}}
    }
};

data_spec(#op_req{operation = update, gri = #gri{aspect = instance}}) -> #{
    at_least_one => #{
        <<"state">> => {atom, [?ATTACHED_DATASET, ?DETACHED_DATASET]},
        <<"setProtectionFlags">> => {list_of_binaries, ?ALL_PROTECTION_FLAGS},
        <<"unsetProtectionFlags">> => {list_of_binaries, ?ALL_PROTECTION_FLAGS}
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
fetch_entity(#op_req{auth = ?NOBODY}) ->
    ?ERROR_UNAUTHORIZED;

fetch_entity(#op_req{operation = Op, auth = ?USER(_UserId), gri = #gri{
    id = DatasetId,
    aspect = As,
    scope = private
}}) when
    (Op =:= get andalso As =:= instance);
    (Op =:= get andalso As =:= children);
    (Op =:= get andalso As =:= children_details);
    (Op =:= get andalso As =:= archives);
    (Op =:= get andalso As =:= archives_details);
    (Op =:= update andalso As =:= instance);
    (Op =:= delete andalso As =:= instance)
->
    case dataset:get(DatasetId) of
        {ok, DatasetDoc} ->
            {ok, {DatasetDoc, 1}};
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback authorize/2.
%%
%% Checks only membership in space. Dataset management privileges
%% are checked later by fslogic layer.
%% @end
%%--------------------------------------------------------------------
-spec authorize(middleware:req(), middleware:entity()) -> boolean().
authorize(#op_req{operation = create, auth = Auth, gri = #gri{aspect = instance}, data = Data}, _) ->
    RootFileGuid = maps:get(<<"rootFileId">>, Data),
    SpaceId = file_id:guid_to_space_id(RootFileGuid),
    middleware_utils:is_eff_space_member(Auth, SpaceId);

authorize(#op_req{operation = Op, auth = Auth, gri = #gri{aspect = As}}, DatasetDoc) when
    (Op =:= get andalso As =:= instance);
    (Op =:= get andalso As =:= children);
    (Op =:= get andalso As =:= children_details);
    (Op =:= get andalso As =:= archives);
    (Op =:= get andalso As =:= archives_details);
    (Op =:= update andalso As =:= instance);
    (Op =:= delete andalso As =:= instance)
->
    {ok, SpaceId} = dataset:get_space_id(DatasetDoc),
    middleware_utils:is_eff_space_member(Auth, SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback validate/2.
%% @end
%%--------------------------------------------------------------------
-spec validate(middleware:req(), middleware:entity()) -> ok | no_return().
validate(#op_req{operation = create, gri = #gri{aspect = instance}, data = Data}, _) ->
    RootFileGuid = maps:get(<<"rootFileId">>, Data),
    SpaceId = file_id:guid_to_space_id(RootFileGuid),
    middleware_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = Op, gri = #gri{aspect = As}}, DatasetDoc) when
    (Op =:= get andalso As =:= instance);
    (Op =:= get andalso As =:= children);
    (Op =:= get andalso As =:= children_details);
    (Op =:= get andalso As =:= archives);
    (Op =:= get andalso As =:= archives_details);
    (Op =:= update andalso As =:= instance);
    (Op =:= delete andalso As =:= instance)
->
    {ok, SpaceId} = dataset:get_space_id(DatasetDoc),
    middleware_utils:assert_space_supported_locally(SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(middleware:req()) -> middleware:create_result().
create(#op_req{auth = Auth, data = Data, gri = #gri{aspect = instance} = GRI}) ->
    SessionId = Auth#auth.session_id,
    FileRef = ?FILE_REF(maps:get(<<"rootFileId">>, Data)),
    ProtectionFlags = file_meta:protection_flags_from_json(
        maps:get(<<"protectionFlags">>, Data, [])
    ),
    DatasetId = opl_datasets:establish(SessionId, FileRef, ProtectionFlags),
    DatasetInfo = opl_datasets:get_info(SessionId, DatasetId),
    {ok, resource, {GRI#gri{id = DatasetId}, DatasetInfo}}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(middleware:req(), middleware:entity()) -> middleware:get_result().
get(#op_req{auth = Auth, gri = #gri{id = DatasetId, aspect = instance}}, _) ->
    {ok, opl_datasets:get_info(Auth#auth.session_id, DatasetId)};

get(#op_req{auth = Auth, gri = #gri{id = DatasetId, aspect = Aspect}, data = Data}, _)
    when Aspect =:= children
    orelse Aspect =:= children_details
->
    ListingMode = case Aspect of
        children -> ?BASIC_INFO;
        children_details -> ?EXTENDED_INFO
    end,
    {ok, value, opl_datasets:list_children_datasets(
        Auth#auth.session_id, DatasetId, gather_listing_opts(Data), ListingMode
    )};

get(#op_req{auth = Auth, gri = #gri{id = DatasetId, aspect = Aspect}, data = Data}, _)
    when Aspect =:= archives
    orelse Aspect =:= archives_details
->
    ListingMode = case Aspect of
        archives -> ?BASIC_INFO;
        archives_details -> ?EXTENDED_INFO
    end,
    {ok, value, opl_archives:list(
        Auth#auth.session_id, DatasetId, gather_listing_opts(Data), ListingMode
    )}.

%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback update/1.
%% @end
%%--------------------------------------------------------------------
-spec update(middleware:req()) -> middleware:update_result().
update(#op_req{auth = Auth, gri = #gri{id = DatasetId, aspect = instance}, data = Data}) ->
    opl_datasets:update(
        Auth#auth.session_id, DatasetId,
        maps:get(<<"state">>, Data, undefined),
        file_meta:protection_flags_from_json(maps:get(<<"setProtectionFlags">>, Data, [])),
        file_meta:protection_flags_from_json(maps:get(<<"unsetProtectionFlags">>, Data, []))
    ).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(middleware:req()) -> middleware:delete_result().
delete(#op_req{auth = Auth, gri = #gri{id = DatasetId, aspect = instance}}) ->
    opl_datasets:remove(Auth#auth.session_id, DatasetId).


%%%===================================================================
%%% Util functions
%%%===================================================================

-spec gather_listing_opts(middleware:data()) -> dataset_api:listing_opts().
gather_listing_opts(Data) ->
    Opts = #{limit => maps:get(<<"limit">>, Data, ?DEFAULT_LIST_LIMIT)},
    case maps:get(<<"token">>, Data, undefined) of
        undefined ->
            Opts2 = Opts#{offset => maps:get(<<"offset">>, Data, 0)},
            maps_utils:put_if_defined(Opts2, start_index, maps:get(<<"index">>, Data, undefined));
        Token when is_binary(Token) ->
            % if token is passed, offset has to be increased by 1
            % to ensure that listing using token is exclusive
            Opts#{
                start_index => http_utils:base64url_decode(Token),
                offset => maps:get(<<"offset">>, Data, 0) + 1
            }
    end.