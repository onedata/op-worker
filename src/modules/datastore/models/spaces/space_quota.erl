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

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([create/1, get/1, delete/1, update/2]).
-export([apply_size_change/2, available_size/1, assert_write/1, assert_write/2,
    get_disabled_spaces/0, apply_size_change_and_maybe_emit/2, current_size/1,
    create_or_update/2, get_space_id/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_posthooks/0, get_record_version/0,
    upgrade_record/2]).

-type id() :: od_space:id().
-type diff() :: datastore_doc:diff(record()).
-type record() :: #space_quota{}.
-type doc() :: datastore_doc:doc(record()).
-export_type([id/0, record/0, doc/0]).

-define(CTX, #{model => ?MODULE}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates space quota.
%% @end
%%--------------------------------------------------------------------
-spec create(doc()) -> {ok, id()} | {error, term()}.
create(Doc) ->
    ?extract_key(datastore_model:create(?CTX, Doc)).

%%--------------------------------------------------------------------
%% @doc
%% Returns space quota.
%% @end
%%--------------------------------------------------------------------
-spec get(id()) -> {ok, doc()} | {error, term()}.
get(SpaceId) ->
    case datastore_model:get(?CTX, SpaceId) of
        {ok, Doc} ->
            {ok, Doc};
        {error, not_found} ->
            case space_quota:create(default_doc(SpaceId)) of
                {ok, _} -> space_quota:get(SpaceId);
                {error, already_exists} -> space_quota:get(SpaceId);
                {error, Reason} -> {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

-spec update(id(), diff()) -> {ok, doc()}.
update(SpaceId, UpdateFun) ->
    datastore_model:update(?CTX, SpaceId, UpdateFun).

-spec create_or_update(id(), diff()) -> {ok, doc()}.
create_or_update(SpaceId, UpdateFun) ->
    {ok, DefaultValue} = UpdateFun(#space_quota{}),
    datastore_model:update(?CTX, SpaceId, UpdateFun, default_doc(SpaceId, DefaultValue)).

%%--------------------------------------------------------------------
%% @doc
%% Deletes space quota.
%% @end
%%--------------------------------------------------------------------
-spec delete(id()) -> ok | {error, term()}.
delete(Key) ->
    datastore_model:delete(?CTX, Key).

-spec get_space_id(doc()) -> id().
get_space_id(#document{key = SpaceId, value = #space_quota{}}) ->
    SpaceId.

%%--------------------------------------------------------------------
%% @doc
%% Records total space size change.
%% @end
%%--------------------------------------------------------------------
-spec apply_size_change(id(), integer()) -> {ok, doc()} | {error, term()}.
apply_size_change(SpaceId, SizeDiff) ->
    Diff = fun(Quota = #space_quota{current_size = Size}) ->
        {ok, Quota#space_quota{current_size = Size + SizeDiff}}
    end,
    create_or_update(SpaceId, Diff).


%%--------------------------------------------------------------------
%% @doc
%% Records total space size change. If space becomes accessible or
%% is getting disabled because of this change, QuotaExceeded event is sent.
%% @end
%%--------------------------------------------------------------------
-spec apply_size_change_and_maybe_emit(id(), integer()) -> ok | {error, any()}.
apply_size_change_and_maybe_emit(_SpaceId, 0) ->
    ok;
apply_size_change_and_maybe_emit(SpaceId, SizeDiff) ->
    {ok, _} = space_quota:apply_size_change(SpaceId, SizeDiff),
    Before = space_quota:available_size(SpaceId),
    After = space_quota:available_size(SpaceId),
    case Before * After =< 0 of
        true -> fslogic_event_emitter:emit_quota_exceeded();
        false -> ok
    end.

%%-------------------------------------------------------------------
%% @doc
%% Returns current storage occupancy.
%% @end
%%-------------------------------------------------------------------
-spec current_size(id()) -> non_neg_integer().
current_size(#document{value = SQ}) ->
    current_size(SQ);
current_size(#space_quota{current_size = CurrentSize}) ->
    CurrentSize;
current_size(SpaceId) ->
    case space_quota:get(SpaceId) of
        {ok, #document{value = SQ}} ->
            current_size(SQ);
        {error, not_found} ->
            0
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns current available size of given space.
%% Values below 0 mean that there are more bytes written to the space
%% then quota allows.
%% @end
%%--------------------------------------------------------------------
-spec available_size(id()) -> integer().
available_size(SpaceId) ->
    try
        {ok, SupSize} = provider_logic:get_support_size(SpaceId),
        CSize = ?MODULE:current_size(SpaceId),
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
-spec assert_write(SpaceId :: id()) -> ok | no_return().
assert_write(SpaceId) ->
    space_quota:assert_write(SpaceId, 1).


%%--------------------------------------------------------------------
%% @doc
%% Checks if write operation with given size is permitted for given space.
%% @end
%%--------------------------------------------------------------------
-spec assert_write(SpaceId :: id(), WriteSize :: integer()) -> ok | no_return().
assert_write(_SpaceId, WriteSize) when WriteSize =< 0 ->
    ok;
assert_write(SpaceId, _WriteSize) ->
    case space_quota:available_size(SpaceId) > 0 of
        true -> ok;
        false -> throw(?ENOSPC)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns list of spaces that are currently over quota limit.
%% @end
%%--------------------------------------------------------------------
-spec get_disabled_spaces() -> [id()] | {error, term()}.
get_disabled_spaces() ->
    case provider_logic:get_spaces() of
        {ok, SpaceIds} ->
            SpacesWithASize = lists:map(fun(SpaceId) ->
                {SpaceId, catch space_quota:available_size(SpaceId)}
            end, SpaceIds),

            {ok, [
                SpaceId || {SpaceId, AvailableSize} <- SpacesWithASize,
                AvailableSize =< 0 orelse not is_integer(AvailableSize)
            ]};
        {error, _} = Error ->
            Error
    end.

%%-------------------------------------------------------------------
%% @doc
%% Posthook responsible for checking whether auto-cleaning run
%% should be triggered. If true, the function also starts
%% auto-cleaning run.
%% @end
%%-------------------------------------------------------------------
-spec autocleaning_check_posthook(atom(), term(), term()) -> term().
autocleaning_check_posthook(create, _, Result = {ok, #document{key = SpaceId}}) ->
    autocleaning_checker:check(SpaceId),
    Result;
autocleaning_check_posthook(update, _, Result = {ok, #document{key = SpaceId}}) ->
    autocleaning_checker:check(SpaceId),
    Result;
autocleaning_check_posthook(_, _, Result) ->
    Result.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec default_doc(id()) -> doc().
default_doc(SpaceId) ->
    default_doc(SpaceId, #space_quota{}).

-spec default_doc(id(), record()) -> doc().
default_doc(SpaceId, DefaultValue) ->
    #document{
        key = SpaceId,
        value = DefaultValue,
        scope = SpaceId
    }.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    3.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {current_size, integer}
    ]};
get_record_struct(2) ->
    {record, [
        {current_size, integer},
        {last_autocleaning_check, integer}
    ]};
get_record_struct(3) ->
    {record, [
        {current_size, integer}
    ]}.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, {?MODULE, CurrentSize}) ->
    {2, {?MODULE, CurrentSize, 0}};
upgrade_record(2, {?MODULE, CurrentSize, _LastAutocleaningCheck}) ->
    {3, {?MODULE, CurrentSize}}.

%%--------------------------------------------------------------------
%% @doc
%% Returns list of callbacks which will be called after each operation
%% on datastore model.
%% @end
%%--------------------------------------------------------------------
-spec get_posthooks() -> [datastore_hooks:posthook()].
get_posthooks() ->
    [fun autocleaning_check_posthook/3].