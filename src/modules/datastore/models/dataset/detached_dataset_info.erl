%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% WRITEME
%%% @end
%%%-------------------------------------------------------------------
-module(detached_dataset_info).
-author("Jakub Kudzia").

%% API
-export([create_info/3, get_path/1, get_file_root_path/1, get_name/1, get_file_root_type/1]).

-type info() :: #info{}.

-export_type([info/0]).

-record(info, {
    dataset_path :: dataset:path(),
    file_root_path :: file_meta:path(),
    file_root_type :: file_meta:type()
    % todo protection flags
}).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec create_info(dataset:path(), file_meta:path(), file_meta:type()) -> info().
create_info(DatasetPath, FileRootPath, FileRootType) ->
    #info{
        dataset_path = DatasetPath,
        file_root_path = FileRootPath,
        file_root_type = FileRootType
    }.


-spec get_path(info()) -> dataset:path().
get_path(#info{dataset_path = DatasetPath}) ->
    DatasetPath.


-spec get_file_root_path(info()) -> file_meta:path().
get_file_root_path(#info{file_root_path = FileRootPath}) ->
    FileRootPath.


-spec get_name(info()) -> file_meta:name().
get_name(Info) ->
    FileRootPath = get_file_root_path(Info),
    filename:basename(FileRootPath).


-spec get_file_root_type(info()) -> file_meta:type().
get_file_root_type(#info{file_root_type = FileRootType}) ->
    FileRootType.