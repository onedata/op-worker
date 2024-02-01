%%%-------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @doc
%%% Util functions for performing operations concerning storage import tests
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import_utils).
-author("Katarzyna Such").

-include_lib("storage_import_utils.hrl").

%% API
-export([create_storage/2]).


create_storage(Provider, DataRecord) ->
    panel_test_rpc:add_storage(Provider, get_posix_data_from_record(DataRecord)).


%% @private
get_posix_data_from_record(DataRecord) ->
    #storage_spec{name = Name, params = ParamsRecord} = DataRecord,
    #posix_storage_params{type = Type, mountPoint = MountPoint} = ParamsRecord,
    #{atom_to_binary(Name) => #{<<"type">> => Type, <<"mountPoint">> => MountPoint}}.
