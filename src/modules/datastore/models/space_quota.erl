%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Model holding current quota state all supported spaces.
%%% @end
%%%-------------------------------------------------------------------
-module(space_quota).
-author("Rafal Slota").
-behaviour(model_behaviour).

-include("modules/datastore/datastore_specific_models_def.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").

%% API
-export([
    apply_size_change/2, available_size/1, assert_write/1, assert_write/2,
    get_disabled_spaces/0, apply_size_change_and_maybe_emit/2, soft_assert_write/2
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
-spec save(datastore:document()) -> {ok, datastore:key()} | datastore:generic_error().
save(Document) ->
    datastore:save(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:key()} | datastore:update_error().
update(Key, Diff) ->
    datastore:update(?STORE_LEVEL, ?MODULE, Key, Diff).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) -> {ok, datastore:key()} | datastore:create_error().
create(Document) ->
    datastore:create(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    case datastore:get(?STORE_LEVEL, ?MODULE, Key) of
        {error, {not_found, _}} ->
            %% Create empty entry
            case create(#document{key = Key, value = #space_quota{current_size = 0}}) of
                {ok, _} ->
                    get(Key);
                {error, already_exists} ->
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
-spec delete(datastore:key()) -> ok | datastore:generic_error().
delete(Key) ->
    datastore:delete(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:key()) -> datastore:exists_return().
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

%%--------------------------------------------------------------------
%% @doc
%% Records total space size change.
%% @end
%%--------------------------------------------------------------------
-spec apply_size_change(SpaceId :: space_info:id(), SizeDiff :: integer()) ->
    {ok, datastore:key()} | datastore:update_error().
apply_size_change(SpaceId, SizeDiff) ->
    % TODO - use create_or_update
    critical_section:run([?MODEL_NAME, term_to_binary({quota, SpaceId})],
        fun() ->
            {ok, #document{value = Quot = #space_quota{current_size = OldSize}} = Doc} = get(SpaceId),
            save(Doc#document{value = Quot#space_quota{current_size = OldSize + SizeDiff}})
        end).


%%--------------------------------------------------------------------
%% @doc
%% Records total space size change. If space becomes accessible or
%% is getting disabled because of this change, QuotaExeeded event is sent.
%% @end
%%--------------------------------------------------------------------
-spec apply_size_change_and_maybe_emit(SpaceId :: space_info:id(), SizeDiff :: integer()) ->
    ok | {error, Reason :: any()}.
apply_size_change_and_maybe_emit(SpaceId, SizeDiff) ->
    Before = space_quota:available_size(SpaceId),
    {ok, _} = space_quota:apply_size_change(SpaceId, SizeDiff),
    After = space_quota:available_size(SpaceId),
    case Before * After =< 0 of
        true -> fslogic_event:emit_quota_exeeded();
        false -> ok
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns current available size of given space. Values below 0 mean that there are more
%% bytes written to the space then quota allows.
%% @end
%%--------------------------------------------------------------------
-spec available_size(SpaceId :: space_info:id()) ->
    AvailableSize :: integer().
available_size(SpaceId) ->
    try
        {ok, #document{value = #space_quota{current_size = CSize}}} = get(SpaceId),
        {ok, #document{value = #space_info{providers_supports = ProvSupport}}} = space_info:get(SpaceId),
        SupSize = proplists:get_value(oneprovider:get_provider_id(), ProvSupport, 0),
        SupSize - CSize
    catch
        _:Reason ->
            ?error_stacktrace("Unable to calculate quota due to: ~p", [Reason]),
            throw({unable_to_calc_quota, Reason})
    end.


%%--------------------------------------------------------------------
%% @doc
%% Checks if any non-empty write operation is permitted for given space.
%% @equiv assert_write(SpaceId, 1)
%% @end
%%--------------------------------------------------------------------
-spec assert_write(SpaceId :: space_info:id()) ->
    ok | no_return().
assert_write(SpaceId) ->
    space_quota:assert_write(SpaceId, 1).


%%--------------------------------------------------------------------
%% @doc
%% Checks if write operation with given size is permitted for given space.
%% @end
%%--------------------------------------------------------------------
-spec assert_write(SpaceId :: space_info:id(), WriteSize :: integer()) ->
    ok | no_return().
assert_write(_SpaceId, WriteSize) when WriteSize =< 0 ->
    ok;
assert_write(SpaceId, WriteSize) ->
    case space_quota:available_size(SpaceId) >= WriteSize of
        true -> ok;
        false -> throw(?ENOSPC)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Checks if write operation with given size is permitted for given space while taking into
%% consideration soft quota limit set in op_worker configuration.
%% @end
%%--------------------------------------------------------------------
-spec soft_assert_write(SpaceId :: space_info:id(), WriteSize :: integer()) ->
    ok | no_return().
soft_assert_write(_SpaceId, WriteSize) when WriteSize =< 0 ->
    ok;
soft_assert_write(SpaceId, WriteSize) ->
    {ok, SoftQuotaSize} = application:get_env(?APP_NAME, soft_quota_limit_size),
    case space_quota:available_size(SpaceId) + SoftQuotaSize >= WriteSize of
        true -> ok;
        false -> throw(?ENOSPC)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns list of spaces that are currently over quota limit.
%% @end
%%--------------------------------------------------------------------
-spec get_disabled_spaces() -> [space_info:id()].
get_disabled_spaces() ->
    %% @todo: use locally cached data after resolving VFS-2087
    {ok, SpaceIds} = oz_providers:get_spaces(provider),
    SpacesWithASize = lists:map(fun(SpaceId) ->
        {SpaceId, catch space_quota:available_size(SpaceId)}
                                end, SpaceIds),

    [SpaceId || {SpaceId, AvailableSize} <- SpacesWithASize,
        AvailableSize =< 0 orelse not is_integer(AvailableSize)].