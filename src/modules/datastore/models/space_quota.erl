%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Cache for space details fetched from Global Registry.
%%% @end
%%%-------------------------------------------------------------------
-module(space_quota).
-author("Rafal Slota").
-behaviour(model_behaviour).

-include("proto/common/credentials.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("storage_file_manager_errors.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("proto/oneclient/common_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/oz/oz_spaces.hrl").

%% API
-export([
    apply_size_change/2, available_size/1, assert_write/1, assert_write/2,
    get_disabled_spaces/0, apply_size_change_and_maybe_emit/2
]).

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1, model_init/0,
    'after'/5, before/4]).

-type id() :: binary().

-export_type([id/0]).

%%%===================================================================
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1.
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document()) -> {ok, datastore:ext_key()} | datastore:generic_error().
save(Document) ->
    datastore:save(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(datastore:document(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
create_or_update(Doc, Diff) ->
    datastore:create_or_update(?STORE_LEVEL, Doc, Diff).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:ext_key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
update(Key, Diff) ->
    datastore:update(?STORE_LEVEL, ?MODULE, Key, Diff).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) -> {ok, datastore:ext_key()} | datastore:create_error().
create(Document) ->
    datastore:create(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:ext_key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    case datastore:get(?STORE_LEVEL, ?MODULE, Key) of
        {error, {not_found, _}} ->
            case apply_size_change(Key, 0) of
                {ok, _} ->
                    get(Key);
                Other0 ->
                    Other0
            end;
        Other1 ->
            Other1
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:ext_key()) -> ok | datastore:generic_error().
delete(Key) ->
    datastore:delete(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:ext_key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(datastore:exists(?STORE_LEVEL, ?MODULE, Key)).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    ?MODEL_CONFIG(space_quota_bucket, [], ?GLOBALLY_CACHED_LEVEL).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5.
%% @end
%%--------------------------------------------------------------------
-spec 'after'(ModelName :: model_behaviour:model_type(), Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok.
'after'(_ModelName, _Method, _Level, _Context, _ReturnValue) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback before/4.
%% @end
%%--------------------------------------------------------------------
-spec before(ModelName :: model_behaviour:model_type(), Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term()) -> ok | datastore:generic_error().
before(_ModelName, _Method, _Level, _Context) ->
    ok.

%%%===================================================================
%%% API
%%%===================================================================


apply_size_change(SpaceId, SizeDiff) ->
    create_or_update(#document{key = SpaceId, value = #space_quota{current_size = 0}},
        fun(#space_quota{current_size = OldSize}) ->
            {ok, #space_quota{current_size = OldSize + SizeDiff}}
        end
    ).

apply_size_change_and_maybe_emit(SpaceId, SizeDiff) ->
    Before = available_size(SpaceId),
    {ok, _} = apply_size_change(SpaceId, SizeDiff),
    After = available_size(SpaceId),
    case Before * After =< 0 of
        true -> fslogic_event:emit_quota_exeeded();
        false -> ok
    end.


available_size(SpaceId) ->
    try
        {ok, #document{value = #space_quota{current_size = CSize}}} = get(SpaceId),
        {ok, #document{value = #space_info{providers_supports = ProvSupport}}} = space_info:get(SpaceId),
        SupSize = proplists:get_value(oneprovider:get_provider_id(), ProvSupport, 0),
        SupSize - CSize
    catch
        _:Reason ->
            ?error_stacktrace("Unable to calculate quota due to: ~p", [Reason]),
            0
    end.

assert_write(SpaceId) ->
    assert_write(SpaceId, 1).

assert_write(_SpaceId, WriteSize) when WriteSize =< 0 ->
    ok;
assert_write(SpaceId, WriteSize) ->
    ?info("assert_write ~p ~p", [SpaceId, WriteSize]),
    case available_size(SpaceId) >= WriteSize of
        true -> ok;
        false -> throw(?ENOSPC)
    end.


get_disabled_spaces() ->
    {ok, SpaceIds} = oz_providers:get_spaces(provider),
    SpacesWithASize = lists:map(fun(SpaceId) ->
        {SpaceId, space_quota:available_size(SpaceId)}
                                end, SpaceIds),

    [SpaceId || {SpaceId, AvailableSize} <- SpacesWithASize, AvailableSize =< 0].