%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Special dirs are directories that have deterministic uuid and can behave
%%% differently than other directories.
%%% For their place in file_tree @see file_tree.
%%%
%%% When links are created for a parent of a special dir they must be created on
%%% all providers, because there can be race on creation as uuid is known - this
%%% can result in broken children listing.
%%5
%%% @TODO VFS-12229 - implement is operation allowed for special dirs subtree
%%% @TODO VFS-12233 - properly handle special dirs deletion
%%% @end
%%%-------------------------------------------------------------------
-module(special_dirs).
-author("Michal Stanisz").

-include_lib("ctool/include/logging.hrl").

-export([set_up_for_new_space/1, report_new_user/1]).
-export([exists/1, is_special/1, is_scope_root_dir/1, is_operation_allowed/2, is_restricted_for_datasets/1,
    is_harvested/1, is_ignored_in_dir_stats/1, is_ignored_in_events/1, is_without_parent/1]).
-export([get_file_meta_if_special/1, get_times_if_special/2]).

-define(ALL_SPECIAL_DIRS, [
    global_root_dir,
    user_root_dir,
    space_dir,
    opened_deleted_files_dir,
    share_root_dir,
    space_archives_root_dir,
    dataset_archives_root_dir,
    archive_dir,
    tmp_dir,
    trash_dir
]).


%%%===================================================================
%%% API
%%%===================================================================

% NOTE: this function MUST be idempotent.
-spec set_up_for_new_space(od_space:id()) -> ok.
set_up_for_new_space(SpaceId) ->
    space_dir:ensure_exists(SpaceId),
    trash_dir:ensure_exists(SpaceId),
    space_archives_root_dir:ensure_exists(SpaceId),
    tmp_dir:ensure_exists(SpaceId),
    opened_deleted_files_dir:ensure_exists(SpaceId).


% NOTE: this function MUST be idempotent.
-spec report_new_user(od_user:id()) -> ok.
report_new_user(UserId) ->
    user_root_dir:ensure_exists(UserId).


-spec exists(file_meta:uuid()) -> boolean() | not_special.
exists(Uuid) ->
    case apply_if_special(Uuid, ?FUNCTION_NAME, [Uuid]) of
        {special, Res} -> Res;
        not_special -> not_special
    end.


-spec is_special(file_meta:uuid()) -> boolean().
is_special(Uuid) ->
    case extract_special_module(Uuid) of
        {true, _} -> true;
        undefined -> false
    end.


-spec is_operation_allowed(file_meta:uuid(), atom()) -> boolean().
is_operation_allowed(Uuid, Operation) ->
    case apply_if_special(Uuid, ?FUNCTION_NAME, [Operation]) of
        {special, Res} -> Res;
        not_special -> true
    end.


-spec is_scope_root_dir(file_meta:uuid()) -> boolean().
is_scope_root_dir(Uuid) ->
    case apply_if_special(Uuid, ?FUNCTION_NAME, []) of
        {special, Res} -> Res;
        not_special -> false
    end.


-spec is_restricted_for_datasets(file_meta:uuid()) -> boolean().
is_restricted_for_datasets(Uuid) ->
    case apply_if_special(Uuid, ?FUNCTION_NAME, []) of
        {special, Res} -> Res;
        not_special -> false
    end.


-spec is_harvested(file_meta:uuid()) -> boolean().
is_harvested(Uuid) ->
    case apply_if_special(Uuid, ?FUNCTION_NAME, []) of
        {special, Res} -> Res;
        not_special -> false
    end.


-spec is_ignored_in_dir_stats(file_meta:uuid()) -> boolean().
is_ignored_in_dir_stats(Uuid) ->
    case apply_if_special(Uuid, ?FUNCTION_NAME, []) of
        {special, Res} -> Res;
        not_special -> false
    end.


-spec is_ignored_in_events(file_meta:uuid()) -> boolean().
is_ignored_in_events(Uuid) ->
    case apply_if_special(Uuid, ?FUNCTION_NAME, []) of
        {special, Res} -> Res;
        not_special -> false
    end.


-spec is_without_parent(file_meta:uuid()) -> boolean().
is_without_parent(Uuid) ->
    case apply_if_special(Uuid, ?FUNCTION_NAME, []) of
        {special, Res} -> Res;
        not_special -> false
    end.


-spec get_times_if_special(file_id:file_guid(), [times_api:times_type()]) -> {true, times:record()} | not_special.
get_times_if_special(Guid, RequestedTimes) ->
    Uuid = file_id:guid_to_uuid(Guid),
    case extract_special_module(Uuid) of
        {true, Module} ->
            case erlang:function_exported(Module, get_times, 2) of
                true ->
                    {true, Module:get_times(Uuid, RequestedTimes)};
                false ->
                    {true, times_api:get(file_ctx:new_by_guid(Guid), RequestedTimes)}
            end;
        undefined ->
            not_special
    end.


-spec get_file_meta_if_special(file_meta:uuid()) -> {true, {ok, file_meta:doc() | {error, term()}}} | not_special.
get_file_meta_if_special(Uuid) ->
    case extract_special_module(Uuid) of
        {true, Module} ->
            case erlang:function_exported(Module, get_file_meta, 1) of
                true ->
                    {true, {ok, Module:get_file_meta(Uuid)}};
                false ->
                    %% @TODO VFS-12230 - remove this hack after special dirs are no longer checked in file meta
                    {true, datastore_model:get((file_meta:get_ctx())#{include_deleted => true}, Uuid)}
            end;
        undefined ->
            not_special
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec apply_if_special(file_meta:uuid(), atom(), [any()]) -> {special, any()} | not_special.
apply_if_special(Uuid, Fun, Args) ->
    case extract_special_module(Uuid) of
        {true, DirType} ->
            {special, erlang:apply(DirType, Fun, Args)};
        undefined ->
            not_special
    end.


%% @private
-spec extract_special_module(file_meta:uuid()) -> {true, module()} | undefined.
extract_special_module(Uuid) ->
    case lists:filter(fun(DirType) -> DirType:is_special(uuid, Uuid) end, ?ALL_SPECIAL_DIRS) of
        [] -> undefined;
        [DirType] -> {true, DirType}
    end.
