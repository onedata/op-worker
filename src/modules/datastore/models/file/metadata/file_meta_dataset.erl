%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module for operating on dataset information stored in
%%% file_meta model.
%%% @end
%%%-------------------------------------------------------------------
-module(file_meta_dataset).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/dataset/dataset.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include_lib("ctool/include/errors.hrl").


%% API
-export([establish/2, reattach/3, detach/1, remove/1]).
-export([get_state/1, is_attached/1, get_id/1, get_id_if_attached/1]).


%%%===================================================================
%%% API functions
%%%===================================================================

-spec establish(file_meta:uuid(), data_access_control:bitmask()) -> ok | {error, term()}.
establish(Uuid, ProtectionFlags) ->
    case file_meta:validate_protection_flags(ProtectionFlags) of
        ok ->
            ?extract_ok(file_meta:update(Uuid, fun
                (FileMeta = #file_meta{dataset_state = undefined}) ->
                    {ok, FileMeta#file_meta{
                        dataset_state = ?ATTACHED_DATASET,
                        protection_flags = ProtectionFlags
                    }};
                (_) ->
                    ?ERROR_ALREADY_EXISTS
            end));
        {error, _} = Error ->
            Error
    end.


-spec reattach(file_meta:uuid(), data_access_control:bitmask(), data_access_control:bitmask()) ->
    {ok, data_access_control:bitmask()} | {error, term()}.
reattach(Uuid, FlagsToSet, FlagsToUnset) ->
    UpdateAns = file_meta:update(Uuid, fun
        (FileMeta = #file_meta{
            dataset_state = ?DETACHED_DATASET,
            protection_flags = CurrFlags
        }) ->
            NewFlags = ?set_flags(?reset_flags(CurrFlags, FlagsToUnset), FlagsToSet),

            case file_meta:validate_protection_flags(NewFlags) of
                ok ->
                    {ok, FileMeta#file_meta{
                        dataset_state = ?ATTACHED_DATASET,
                        protection_flags = NewFlags
                    }};
                {error, _} = Error ->
                    Error
            end;
        (#file_meta{dataset_state = undefined}) ->
            ?ERROR_NOT_FOUND;
        (#file_meta{dataset_state = ?ATTACHED_DATASET}) ->
            % attached dataset cannot be reattached
            ?ERROR_ALREADY_EXISTS
    end),

    case UpdateAns of
        {ok, #document{value = #file_meta{protection_flags = NewFlags}}} -> {ok, NewFlags};
        _ -> UpdateAns
    end.


-spec detach(file_meta:uuid()) -> ok | {error, term()}.
detach(Uuid) ->
    ?extract_ok(file_meta:update(Uuid, fun
        (FileMeta = #file_meta{dataset_state = ?ATTACHED_DATASET}) ->
            {ok, FileMeta#file_meta{dataset_state = ?DETACHED_DATASET}};
        (#file_meta{dataset_state = undefined}) ->
            ?ERROR_NOT_FOUND;
        (#file_meta{dataset_state = ?DETACHED_DATASET}) ->
            % detached dataset cannot be detached
            ?ERROR_ALREADY_EXISTS
    end)).


-spec remove(file_meta:uuid()) -> ok.
remove(Uuid) ->
    Result = ?extract_ok(file_meta:update(Uuid, fun(FileMeta) ->
        {ok, FileMeta#file_meta{
            dataset_state = undefined,
            protection_flags = ?no_flags_mask
        }}
    end)),
    case Result of
        ok -> ok;
        ?ERROR_NOT_FOUND -> ok
    end.


-spec get_state(file_meta:file_meta() | file_meta:doc()) -> dataset:state() | undefined.
get_state(#document{value = FM}) ->
    get_state(FM);
get_state(#file_meta{dataset_state = DatasetState}) ->
    DatasetState.


-spec is_attached(file_meta:file_meta() | file_meta:doc()) -> boolean().
is_attached(#document{value = FM}) ->
    is_attached(FM);
is_attached(#file_meta{dataset_state = DatasetState}) ->
    DatasetState =:= ?ATTACHED_DATASET.


-spec get_id_if_attached(file_meta:doc()) -> dataset:id() | undefined.
get_id_if_attached(FileDoc) ->
    case is_attached(FileDoc) of
        true -> get_id(FileDoc);
        false -> undefined
    end.


-spec has_dataset_established(file_meta:doc() | file_meta:file_meta()) -> boolean().
has_dataset_established(#file_meta{dataset_state = State}) ->
    State =/= undefined;
has_dataset_established(#document{value = FileMeta}) ->
    has_dataset_established(FileMeta).


-spec get_id(file_meta:doc()) -> dataset:id() | undefined.
get_id(FileDoc = #document{}) ->
    case has_dataset_established(FileDoc) of
        true ->
            {ok, Uuid} = file_meta:get_uuid(FileDoc),
            Uuid;
        false ->
            undefined
    end.
