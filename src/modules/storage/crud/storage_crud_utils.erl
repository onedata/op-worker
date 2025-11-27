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

-include("modules/storage/helpers/helpers.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    verify_configuration/4,
    run_diagnostics/3
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec verify_configuration(storage:id() | storage:name(), storage:readonly(), storage:imported(), helper_config:t()) ->
    ok | no_return().
verify_configuration(IdOrName, Readonly, Imported, HelperConfig) ->
    sanitize_readonly_option(Readonly, Imported, IdOrName),
    check_helper_against_readonly_option(Readonly, HelperConfig),
    check_helper_against_imported_option(Imported, HelperConfig).


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


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec sanitize_readonly_option(storage:readonly(), storage:imported(), storage:id() | storage:name()) ->
    ok | no_return().
sanitize_readonly_option(false, _Imported, _IdOrName) ->
    ok;
sanitize_readonly_option(true, false, IdOrName) ->
    throw(?ERR_REQUIRES_IMPORTED_STORAGE(?err_ctx(), IdOrName));
sanitize_readonly_option(true, true, _IdOrName) ->
    ok.


%% @private
-spec check_helper_against_readonly_option(storage:readonly(), helper_config:t()) ->
    ok | no_return().
check_helper_against_readonly_option(true, _HelperConfig) ->
    ok;
check_helper_against_readonly_option(false, HelperConfig) ->
    case helper_config:is_storage_access_type_supported(HelperConfig, ?READWRITE) of
        false ->
            HelperName = helper_config:get_name(HelperConfig),
            throw(?ERR_REQUIRES_READONLY_STORAGE(?err_ctx(), HelperName));
        true ->
            ok
    end.


%% @private
-spec check_helper_against_imported_option(storage:imported(), helper_config:t()) ->
    ok | no_return().
check_helper_against_imported_option(false, _HelperConfig) ->
    ok;
check_helper_against_imported_option(true, HelperConfig) ->
    case helper_config:is_import_supported(HelperConfig) of
        false ->
            HelperName = helper_config:get_name(HelperConfig),
            throw(?ERR_STORAGE_IMPORT_NOT_SUPPORTED(?err_ctx(), HelperName, ?OBJECT_HELPERS));
        true ->
            ok
    end.
