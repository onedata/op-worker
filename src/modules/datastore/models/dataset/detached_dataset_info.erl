%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module for dataset model.
%%% It encapsulates #info{} record which stores dataset information,
%%% valid at the moment when it was detached.
%%% @end
%%%-------------------------------------------------------------------
-module(detached_dataset_info).
-author("Jakub Kudzia").

%% API
-export([create_info/5]).
-export([get_path/1, get_root_file_path/1, get_root_file_type/1, 
    get_protection_flags/1, get_detachment_reason/1]).
-export([set_detachment_reason/2]).

-record(info, {
    dataset_path :: dataset:path(),
    % path to file which is a root of a dataset (a file to which dataset is attached)
    root_file_path :: file_meta:path(),
    % type of a root file
    root_file_type :: file_meta:type(),
    % flags are stored so that they can be restored when dataset is reattached
    protection_flags :: data_access_control:bitmask(),
    detachment_reason :: dataset:detachment_reason()
}).

-type info() :: #info{}.
-export_type([info/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec create_info(dataset:path(), file_meta:path(), file_meta:type(), 
    data_access_control:bitmask(), dataset:detachment_reason()) -> info().
create_info(DatasetPath, RootFilePath, RootFileType, ProtectionFlags, Reason) ->
    #info{
        dataset_path = DatasetPath,
        root_file_path = RootFilePath,
        root_file_type = RootFileType,
        protection_flags = ProtectionFlags,
        detachment_reason = Reason
    }.


-spec get_path(info()) -> dataset:path().
get_path(#info{dataset_path = DatasetPath}) ->
    DatasetPath.


-spec get_root_file_path(info()) -> file_meta:path().
get_root_file_path(#info{root_file_path = RootFilePath}) ->
    RootFilePath.


-spec get_root_file_type(info()) -> file_meta:type().
get_root_file_type(#info{root_file_type = RootFileType}) ->
    RootFileType.


-spec get_protection_flags(info()) -> data_access_control:bitmask().
get_protection_flags(#info{protection_flags = ProtectionFlags}) ->
    ProtectionFlags.


-spec get_detachment_reason(info()) -> dataset:detachment_reason().
get_detachment_reason(#info{detachment_reason = Reason}) ->
    Reason.


-spec set_detachment_reason(info(), dataset:detachment_reason()) -> info().
set_detachment_reason(Info, Reason) ->
    Info#info{detachment_reason = Reason}.