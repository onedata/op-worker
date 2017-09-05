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
-include_lib("ctool/include/logging.hrl").

%% API
-export([create/1, get/1, delete/1]).
-export([
    apply_size_change/2, available_size/1, assert_write/1, assert_write/2,
    get_disabled_spaces/0, apply_size_change_and_maybe_emit/2, soft_assert_write/2
]).

%% datastore_model callbacks
-export([get_record_struct/1]).

-type id() :: binary().
-type doc() :: datastore:doc().
-export_type([id/0]).

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
            Doc = #document{
                key = SpaceId,
                value = #space_quota{current_size = 0}
            },
            case datastore_model:create(?CTX, Doc) of
                {ok, _} -> space_quota:get(SpaceId);
                {error, already_exists} -> space_quota:get(SpaceId);
                {error, Reason} -> {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Deletes space quota.
%% @end
%%--------------------------------------------------------------------
-spec delete(id()) -> ok | {error, term()}.
delete(Key) ->
    datastore_model:delete(?CTX, Key).

%%--------------------------------------------------------------------
%% @doc
%% Records total space size change.
%% @end
%%--------------------------------------------------------------------
-spec apply_size_change(od_space:id(), integer()) ->
    {ok, doc()} | {error, term()}.
apply_size_change(SpaceId, SizeDiff) ->
    Diff = fun(Quota = #space_quota{current_size = Size}) ->
        {ok, Quota#space_quota{current_size = Size + SizeDiff}}
    end,
    Default = #document{key = SpaceId, value = #space_quota{
        current_size = SizeDiff
    }},
    datastore_model:update(?CTX, SpaceId, Diff, Default).


%%--------------------------------------------------------------------
%% @doc
%% Records total space size change. If space becomes accessible or
%% is getting disabled because of this change, QuotaExeeded event is sent.
%% @end
%%--------------------------------------------------------------------
-spec apply_size_change_and_maybe_emit(SpaceId :: od_space:id(), SizeDiff :: integer()) ->
    ok | {error, Reason :: any()}.
apply_size_change_and_maybe_emit(_SpaceId, 0) ->
    ok;
apply_size_change_and_maybe_emit(SpaceId, SizeDiff) ->
    Before = space_quota:available_size(SpaceId),
    {ok, _} = space_quota:apply_size_change(SpaceId, SizeDiff),
    After = space_quota:available_size(SpaceId),
    case Before * After =< 0 of
        true -> fslogic_event_emitter:emit_quota_exeeded();
        false -> ok
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns current available size of given space. Values below 0 mean that there are more
%% bytes written to the space then quota allows.
%% @end
%%--------------------------------------------------------------------
-spec available_size(SpaceId :: od_space:id()) ->
    AvailableSize :: integer().
available_size(SpaceId) ->
    try
        {ok, #document{value = #space_quota{current_size = CSize}}} =
            space_quota:get(SpaceId),
        {ok, #document{value = #od_space{providers_supports = ProvSupport}}} =
            od_space:get_or_fetch(provider, SpaceId, ?ROOT_USER_ID),
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
-spec assert_write(SpaceId :: od_space:id()) ->
    ok | no_return().
assert_write(SpaceId) ->
    space_quota:assert_write(SpaceId, 1).


%%--------------------------------------------------------------------
%% @doc
%% Checks if write operation with given size is permitted for given space.
%% @end
%%--------------------------------------------------------------------
-spec assert_write(SpaceId :: od_space:id(), WriteSize :: integer()) ->
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
-spec soft_assert_write(SpaceId :: od_space:id(), WriteSize :: integer()) ->
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
-spec get_disabled_spaces() -> [od_space:id()].
get_disabled_spaces() ->
    %% @todo: use locally cached data after resolving VFS-2087
    {ok, SpaceIds} = oz_providers:get_spaces(provider),
    SpacesWithASize = lists:map(fun(SpaceId) ->
        {SpaceId, catch space_quota:available_size(SpaceId)}
    end, SpaceIds),

    [SpaceId || {SpaceId, AvailableSize} <- SpacesWithASize,
        AvailableSize =< 0 orelse not is_integer(AvailableSize)].

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

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
    ]}.