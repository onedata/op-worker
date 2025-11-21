%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions for storage CRUD modules.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_crud_utils).
-author("Bartosz Walkowicz").

-include_lib("ctool/include/logging.hrl").

%% API
-export([
    verify_configuration/4,
    run_diagnostics/3
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec verify_configuration(storage:id() | storage:name(), boolean(), boolean(), helper_config:t()) ->
    ok | no_return().
verify_configuration(IdOrName, Readonly, Imported, HelperConfig) ->
    Config = #{
        readonly => Readonly,
        importedStorage => Imported
    },
    case storage:verify_configuration(IdOrName, Config, HelperConfig) of
        ok -> ok;
        {error, _} = Error -> throw(Error)
    end.


-spec run_diagnostics(helper_config:t(), luma:feed(), boolean()) -> ok | no_return().
run_diagnostics(HelperConfig, LumaFeed, PerformReadWriteTest) ->
    Opts = #{read_write_test => PerformReadWriteTest},

    case storage_detector:run_diagnostics(all_nodes, HelperConfig, LumaFeed, Opts) of
        ok ->
            ok;
        {{error, _} = Error, Details} ->
            ?error("Storage diagnostics failed: ~tp, details: ~tp", [Error, Details]),
            throw(Error)
    end.
