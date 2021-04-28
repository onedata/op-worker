%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles middleware operations (create, get, update, delete)
%%% corresponding archives.
%%% @end
%%%-------------------------------------------------------------------
-module(archive_middleware).
-author("Jakub Kudzia").

-behaviour(middleware_plugin).

-include("middleware/middleware.hrl").
-include("modules/archive/archive.hrl").
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
-export([]).

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
        <<"datasetId">> => {binary, non_empty},
        <<"type">> => {atom, [?FULL_ARCHIVE, ?INCREMENTAL_ARCHIVE]},
        <<"character">> => {atom, [?DIP, ?AIP, ?HYBRID]},
        <<"dataStructure">> => {atom, [?BAGIT, ?SIMPLE_COPY]},
        <<"metadataStructure">> => {atom, [?BUILT_IN, ?JSON, ?XML]}
    },
    optional => #{
        <<"description">> => {binary, any}
    }
};

data_spec(#op_req{operation = get, gri = #gri{aspect = instance}}) ->
    undefined;

data_spec(#op_req{operation = update, gri = #gri{aspect = instance}}) -> #{
    optional => #{
        description => {binary, any}
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
    id = ArchiveId,
    aspect = As,
    scope = private
}}) when
    (Op =:= get andalso As =:= instance);
    (Op =:= update andalso As =:= instance);
    (Op =:= delete andalso As =:= instance)
->
    case archive:get(ArchiveId) of
        {ok, ArchiveDoc} ->
            {ok, {ArchiveDoc, 1}};
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback authorize/2.
%%
%% Checks only membership in space. Archive management privileges
%% are checked later by fslogic layer.
%% @end
%%--------------------------------------------------------------------
-spec authorize(middleware:req(), middleware:entity()) -> boolean().
authorize(#op_req{operation = create, auth = Auth, gri = #gri{aspect = instance}, data = Data}, _) ->
    DatasetId = maps:get(<<"datasetId">>, Data),
    {ok, SpaceId} = dataset:get_space_id(DatasetId),
    middleware_utils:is_eff_space_member(Auth, SpaceId);

authorize(#op_req{operation = Op, auth = Auth, gri = #gri{aspect = As}}, ArchiveDoc) when
    (Op =:= get andalso As =:= instance);
    (Op =:= update andalso As =:= instance);
    (Op =:= delete andalso As =:= instance)
->
    SpaceId = archive:get_space_id(ArchiveDoc),
    middleware_utils:is_eff_space_member(Auth, SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback validate/2.
%% @end
%%--------------------------------------------------------------------
-spec validate(middleware:req(), middleware:entity()) -> ok | no_return().
validate(#op_req{operation = create, gri = #gri{aspect = instance}, data = Data}, _) ->
    DatasetId = maps:get(<<"datasetId">>, Data),
    {ok, SpaceId} = dataset:get_space_id(DatasetId),
    middleware_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = Op, gri = #gri{aspect = As}}, ArchiveDoc) when
    (Op =:= get andalso As =:= instance);
    (Op =:= update andalso As =:= instance);
    (Op =:= delete andalso As =:= instance)
->
    SpaceId = archive:get_space_id(ArchiveDoc),
    middleware_utils:assert_space_supported_locally(SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(middleware:req()) -> middleware:create_result().
create(#op_req{auth = Auth, data = Data, gri = #gri{aspect = instance} = GRI}) ->
    SessionId = Auth#auth.session_id,
    DatasetId = maps:get(<<"datasetId">>, Data),
    Params = #{
        type => maps:get(<<"type">>, Data),
        character => maps:get(<<"character">>, Data),
        data_structure => maps:get(<<"dataStructure">>, Data),
        metadata_structure => maps:get(<<"metadataStructure">>, Data),
        description => maps:get(<<"description">>, Data, <<>>)
    },
    {ok, ArchiveId} = ?check(lfm:archive_dataset(SessionId, DatasetId, Params)),
    {ok, ArchiveInfo} = ?check(lfm:get_archive_info(SessionId, ArchiveId)),
    {ok, resource, {GRI#gri{id = ArchiveId}, ArchiveInfo}}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(middleware:req(), middleware:entity()) -> middleware:get_result().
get(#op_req{auth = Auth, gri = #gri{id = ArchiveId, aspect = instance}}, _) ->
    ?check(lfm:get_archive_info(Auth#auth.session_id, ArchiveId)).

%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback update/1.
%% @end
%%--------------------------------------------------------------------
-spec update(middleware:req()) -> middleware:update_result().
update(#op_req{auth = Auth, gri = #gri{id = ArchiveId, aspect = instance}, data = Data}) ->
    Params = kv_utils:copy_found([{<<"description">>, description}], Data, #{}),
    case map_size(Params) =:= 0 of
        true -> ok;
        false -> ?check(lfm:update_archive(Auth#auth.session_id, ArchiveId, Params))
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(middleware:req()) -> middleware:delete_result().
delete(#op_req{auth = Auth, gri = #gri{id = ArchiveId, aspect = instance}}) ->
    ?check(lfm:remove_archive(Auth#auth.session_id, ArchiveId)).
