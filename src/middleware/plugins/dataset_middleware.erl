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
-module(dataset_middleware).
-author("Bartosz Walkowicz").

-behaviour(middleware_plugin).

-include("middleware/middleware.hrl").
-include("modules/dataset/dataset.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/errors.hrl").

-export([
    operation_supported/3,
    data_spec/1,
    fetch_entity/1,
    authorize/2,
    validate/2
]).
-export([create/1, get/2, update/1, delete/1]).

% Util functions
-export([build_dataset_listing_opts/1, build_list_dataset_response/2, translate_dataset_info/1]).

-define(MAX_LIST_LIMIT, 1000).
-define(DEFAULT_LIST_LIMIT, 100).
-define(ALL_PROTECTION_FLAGS, [?DATA_PROTECTION_BIN, ?METADATA_PROTECTION_BIN]).


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
operation_supported(get, children, private) -> true;
operation_supported(get, children_details, private) -> true;

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
        <<"token">> => {binary, any},
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
%% {@link middleware_plugin} callback fetch_entity/1.
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
%% {@link middleware_plugin} callback authorize/2.
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
    (Op =:= update andalso As =:= instance);
    (Op =:= delete andalso As =:= instance)
->
    {ok, SpaceId} = dataset:get_space_id(DatasetDoc),
    middleware_utils:is_eff_space_member(Auth, SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback validate/2.
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
    (Op =:= update andalso As =:= instance);
    (Op =:= delete andalso As =:= instance)
->
    {ok, SpaceId} = dataset:get_space_id(DatasetDoc),
    middleware_utils:assert_space_supported_locally(SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(middleware:req()) -> middleware:create_result().
create(#op_req{auth = Auth, data = Data, gri = #gri{aspect = instance} = GRI}) ->
    SessionId = Auth#auth.session_id,
    FileRef = ?FILE_REF(maps:get(<<"rootFileId">>, Data)),
    ProtectionFlags = file_meta:protection_flags_from_json(
        maps:get(<<"protectionFlags">>, Data, [])
    ),
    {ok, DatasetId} = ?check(lfm:establish_dataset(SessionId, FileRef, ProtectionFlags)),
    {ok, DatasetInfo} = ?check(lfm:get_dataset_info(SessionId, DatasetId)),
    {ok, resource, {GRI#gri{id = DatasetId}, DatasetInfo}}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(middleware:req(), middleware:entity()) -> middleware:get_result().
get(#op_req{auth = Auth, gri = #gri{id = DatasetId, aspect = instance}}, _) ->
    ?check(lfm:get_dataset_info(Auth#auth.session_id, DatasetId));

get(#op_req{auth = Auth, gri = #gri{id = DatasetId, aspect = Aspect}, data = Data}, _)
    when Aspect =:= children
    orelse Aspect =:= children_details
->
    ListingMode = case Aspect of
        children -> ?BASIC_INFO;
        children_details -> ?EXTENDED_INFO
    end,
    {ok, Datasets, IsLast} = ?check(lfm:list_children_datasets(
        Auth#auth.session_id, DatasetId, build_dataset_listing_opts(Data), ListingMode
    )),
    {ok, value, {Datasets, IsLast}}.

%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback update/1.
%% @end
%%--------------------------------------------------------------------
-spec update(middleware:req()) -> middleware:update_result().
update(#op_req{auth = Auth, gri = #gri{id = DatasetId, aspect = instance}, data = Data}) ->
    Result = lfm:update_dataset(
        Auth#auth.session_id, DatasetId,
        maps:get(<<"state">>, Data, undefined),
        file_meta:protection_flags_from_json(maps:get(<<"setProtectionFlags">>, Data, [])),
        file_meta:protection_flags_from_json(maps:get(<<"unsetProtectionFlags">>, Data, []))
    ),
    case Result of
        {error, ?ENOENT} -> throw(?ERROR_NOT_FOUND);
        {error, ?EEXIST} -> throw(?ERROR_ALREADY_EXISTS);
        Other -> ?check(Other)
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(middleware:req()) -> middleware:delete_result().
delete(#op_req{auth = Auth, gri = #gri{id = DatasetId, aspect = instance}}) ->
    ?check(lfm:remove_dataset(Auth#auth.session_id, DatasetId)).


%%%===================================================================
%%% Util functions
%%%===================================================================

-spec build_dataset_listing_opts(middleware:data()) -> dataset_api:listing_opts().
build_dataset_listing_opts(Data) ->
    Opts = #{limit => maps:get(<<"limit">>, Data, ?DEFAULT_LIST_LIMIT)},
    Token = maps:get(<<"token">>, Data, undefined),
    case Token =:= undefined orelse Token =:= <<>> of
        true ->
            Opts2 = maps_utils:put_if_defined(Opts, offset, maps:get(<<"offset">>, Data, undefined)),
            maps_utils:put_if_defined(Opts2, start_index, maps:get(<<"index">>, Data, undefined));
        false ->
            % if token is passed, offset has to be increased by 1
            % to ensure that listing using token is exclusive
            Opts#{
                start_index => base64url:decode(Token),
                offset => maps:get(<<"offset">>, Data, 0) + 1
            }
    end.


-spec build_list_dataset_response([dataset_api:entries()], boolean()) -> json_utils:json_map().
build_list_dataset_response(Datasets, IsLast) ->
    TranslatedDatasets = lists:map(fun
        (DatasetInfo = #dataset_info{}) ->
            translate_dataset_info(DatasetInfo);
        ({DatasetId, DatasetName, Index}) ->
            #{<<"id">> => DatasetId, <<"name">> => DatasetName, <<"index">> => Index}
    end, Datasets),

    Result = #{
        <<"datasets">> => TranslatedDatasets,
        <<"isLast">> => IsLast
    },
    NextPageToken = case length(TranslatedDatasets) =:= 0 of
        true -> null;
        false -> base64url:encode(maps:get(<<"index">>, lists:last(TranslatedDatasets)))
    end,
    Result#{<<"nextPageToken">> => NextPageToken}.


-spec translate_dataset_info(lfm_datasets:info()) -> json_utils:json_map().
translate_dataset_info(#dataset_info{
    id = DatasetId,
    state = State,
    root_file_guid = RootFileGuid,
    root_file_path = RootFilePath,
    root_file_type = RootFileType,
    creation_time = CreationTime,
    protection_flags = ProtectionFlags,
    parent = ParentId,
    index = Index
}) ->
    #{
        <<"gri">> => gri:serialize(#gri{
            type = op_dataset, id = DatasetId,
            aspect = instance, scope = private
        }),
        <<"parent">> => case ParentId of
            undefined ->
                null;
            _ ->
                gri:serialize(#gri{
                    type = op_dataset, id = ParentId,
                    aspect = instance, scope = private
                })
        end,
        <<"rootFile">> => gri:serialize(#gri{
            type = op_file, id = RootFileGuid,
            aspect = instance, scope = private
        }),
        <<"rootFileType">> => str_utils:to_binary(RootFileType),
        <<"rootFilePath">> => RootFilePath,
        <<"state">> => atom_to_binary(State, utf8),
        <<"protectionFlags">> => file_meta:protection_flags_to_json(ProtectionFlags),
        <<"creationTime">> => CreationTime,
        <<"index">> => Index
    }.