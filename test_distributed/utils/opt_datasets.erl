%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions for manipulating datasets in CT tests.
%%% @end
%%%-------------------------------------------------------------------
-module(opt_datasets).
-author("Bartosz Walkowicz").

-include("modules/fslogic/acl.hrl").
-include("modules/dataset/dataset.hrl").

-export([
    list_top_datasets/5,
    list_children_datasets/4,
    establish/3,
    get_info/3,
    reattach_dataset/3,
    detach_dataset/3,
    update/6,
    remove/3,
    get_file_eff_summary/3
]).

-define(CALL(NodeSelector, Args), test_rpc:call(
    op_worker, NodeSelector, opl_datasets, ?FUNCTION_NAME, Args
)).


%%%===================================================================
%%% API
%%%===================================================================


-spec list_top_datasets(
    oct_background:node_selector(),
    session:id(),
    od_space:id(),
    dataset:state(),
    dataset_api:listing_opts()
) ->
    {ok, {dataset_api:entries(), boolean()}} | errors:error().
list_top_datasets(NodeSelector, SessionId, SpaceId, State, Opts) ->
    ?CALL(NodeSelector, [SessionId, SpaceId, State, Opts, undefined]).


-spec list_children_datasets(
    oct_background:node_selector(),
    session:id(),
    dataset:id(),
    dataset_api:listing_opts()
) ->
    {ok, {dataset_api:entries(), boolean()}} | errors:error().
list_children_datasets(NodeSelector, SessionId, DatasetId, Opts) ->
    ?CALL(NodeSelector, [SessionId, DatasetId, Opts, undefined]).


-spec establish(oct_background:node_selector(), session:id(), lfm:file_key()) ->
    {ok, dataset:id()} | errors:error().
establish(NodeSelector, SessionId, FileKey) ->
    establish(NodeSelector, SessionId, FileKey, 0).


-spec establish(
    oct_background:node_selector(),
    session:id(),
    lfm:file_key(),
    data_access_control:bitmask()
) ->
    {ok, dataset:id()} | errors:error().
establish(NodeSelector, SessionId, FileKey, ProtectionFlags) ->
    ?CALL(NodeSelector, [SessionId, FileKey, ProtectionFlags]).


-spec get_info(oct_background:node_selector(), session:id(), dataset:id()) ->
    {ok, dataset_api:info()} | errors:error().
get_info(NodeSelector, SessionId, DatasetId) ->
    ?CALL(NodeSelector, [SessionId, DatasetId]).


-spec reattach_dataset(oct_background:node_selector(), session:id(), dataset:id()) ->
    ok | errors:error().
reattach_dataset(NodeSelector, SessionId, DatasetId) ->
    update(NodeSelector, SessionId, DatasetId, ?ATTACHED_DATASET, ?no_flags_mask, ?no_flags_mask).


-spec detach_dataset(oct_background:node_selector(), session:id(), dataset:id()) ->
    ok | errors:error().
detach_dataset(NodeSelector, SessionId, DatasetId) ->
    update(NodeSelector, SessionId, DatasetId, ?DETACHED_DATASET, ?no_flags_mask, ?no_flags_mask).


-spec update(
    oct_background:node_selector(),
    session:id(),
    dataset:id(),
    undefined | dataset:state(),
    data_access_control:bitmask(),
    data_access_control:bitmask()
) ->
    ok | errors:error().
update(NodeSelector, SessionId, DatasetId, NewState, FlagsToSet, FlagsToUnset) ->
    ?CALL(NodeSelector, [SessionId, DatasetId, NewState, FlagsToSet, FlagsToUnset]).


-spec remove(oct_background:node_selector(), session:id(), dataset:id()) ->
    ok | errors:error().
remove(NodeSelector, SessionId, DatasetId) ->
    ?CALL(NodeSelector, [SessionId, DatasetId]).


-spec get_file_eff_summary(oct_background:node_selector(), session:id(), lfm:file_key()) ->
    {ok, dataset_api:file_eff_summary()} | errors:error().
get_file_eff_summary(NodeSelector, SessionId, FileKey) ->
    ?CALL(NodeSelector, [SessionId, FileKey]).
