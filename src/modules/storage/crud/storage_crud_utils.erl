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
-include_lib("opw_panel_contracts/include/storage/common.hrl").
-include_lib("opw_panel_contracts/include/storage/ceph.hrl").
-include_lib("opw_panel_contracts/include/storage/common.hrl").
-include_lib("opw_panel_contracts/include/storage/cephrados.hrl").
-include_lib("opw_panel_contracts/include/storage/glusterfs.hrl").
-include_lib("opw_panel_contracts/include/storage/http.hrl").
-include_lib("opw_panel_contracts/include/storage/nfs.hrl").
-include_lib("opw_panel_contracts/include/storage/nulldevice.hrl").
-include_lib("opw_panel_contracts/include/storage/posix.hrl").
-include_lib("opw_panel_contracts/include/storage/s3.hrl").
-include_lib("opw_panel_contracts/include/storage/swift.hrl").
-include_lib("opw_panel_contracts/include/storage/webdav.hrl").
-include_lib("opw_panel_contracts/include/storage/xrootd.hrl").

%% API
-export([
    pretty_print_spec/1,
    verify_configuration/4,
    run_diagnostics/3
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec pretty_print_spec(onedata_storage:create_spec() | onedata_storage:update_spec()) ->
    io_lib:chars().
pretty_print_spec(Spec) ->
    RedactedSpec = redact_confidential_data(Spec),
    io_lib_pretty:print(RedactedSpec, fun get_record_def/2).


-spec verify_configuration(storage:id() | storage:name(), storage:readonly(), storage:imported(), helper_spec:t()) ->
    ok | no_return().
verify_configuration(IdOrName, Readonly, Imported, HelperSpec) ->
    sanitize_readonly_option(Readonly, Imported, IdOrName),
    check_helper_against_readonly_option(Readonly, HelperSpec),
    check_helper_against_imported_option(Imported, HelperSpec).


-spec run_diagnostics(helper_spec:t(), luma:feed(), boolean()) -> ok | no_return().
run_diagnostics(HelperSpec, LumaFeed, PerformReadWriteTest) ->
    Opts = #{read_write_test => PerformReadWriteTest},

    case storage_detector:run_diagnostics(all_nodes, HelperSpec, LumaFeed, Opts) of
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
-spec check_helper_against_readonly_option(storage:readonly(), helper_spec:t()) ->
    ok | no_return().
check_helper_against_readonly_option(true, _HelperSpec) ->
    ok;
check_helper_against_readonly_option(false, HelperSpec) ->
    case helper_spec:is_storage_access_type_supported(HelperSpec, ?READWRITE) of
        false ->
            HelperName = helper_spec:get_name(HelperSpec),
            throw(?ERR_REQUIRES_READONLY_STORAGE(?err_ctx(), HelperName));
        true ->
            ok
    end.


%% @private
-spec check_helper_against_imported_option(storage:imported(), helper_spec:t()) ->
    ok | no_return().
check_helper_against_imported_option(false, _HelperSpec) ->
    ok;
check_helper_against_imported_option(true, HelperSpec) ->
    case helper_spec:is_import_supported(HelperSpec) of
        false ->
            HelperName = helper_spec:get_name(HelperSpec),
            throw(?ERR_STORAGE_IMPORT_NOT_SUPPORTED(?err_ctx(), HelperName, ?OBJECT_HELPERS));
        true ->
            ok
    end.


%% @private
-spec redact_confidential_data(onedata_storage:create_spec() | onedata_storage:update_spec()) ->
    onedata_storage:create_spec() | onedata_storage:update_spec().
redact_confidential_data(Spec = #storage_create_spec{type = Type, credentials = Credentials}) ->
    RedactedCredentials = helper_spec:redact_confidential_credentials(Type, Credentials),
    Spec#storage_create_spec{credentials = RedactedCredentials};
redact_confidential_data(Spec = #storage_update_spec{credentials = undefined}) ->
    Spec;
redact_confidential_data(Spec = #storage_update_spec{type = Type, credentials = CredentialsDiff}) ->
    RedactedCredentialsDiff = helper_spec:redact_confidential_credentials_diff(Type, CredentialsDiff),
    Spec#storage_update_spec{credentials = RedactedCredentialsDiff}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns information about chosen records, such as fields,
%% required to for example pretty print it
%% @end
%%--------------------------------------------------------------------
-spec get_record_def(atom(), non_neg_integer()) -> no | atom().
get_record_def(storage_create_spec, N) ->
    case record_info(size, storage_create_spec) - 1 of
        N -> record_info(fields, storage_create_spec);
        _ -> no
    end;
get_record_def(storage_update_spec, N) ->
    case record_info(size, storage_update_spec) - 1 of
        N -> record_info(fields, storage_update_spec);
        _ -> no
    end;
get_record_def(luma_spec, N) ->
    case record_info(size, luma_spec) - 1 of
        N -> record_info(fields, luma_spec);
        _ -> no
    end;
get_record_def(ceph_helper_credentials, N) ->
    case record_info(size, ceph_helper_credentials) - 1 of
        N -> record_info(fields, ceph_helper_credentials);
        _ -> no
    end;
get_record_def(ceph_helper_credentials_diff, N) ->
    case record_info(size, ceph_helper_credentials_diff) - 1 of
        N -> record_info(fields, ceph_helper_credentials_diff);
        _ -> no
    end;
get_record_def(ceph_helper_configuration, N) ->
    case record_info(size, ceph_helper_configuration) - 1 of
        N -> record_info(fields, ceph_helper_configuration);
        _ -> no
    end;
get_record_def(ceph_helper_configuration_diff, N) ->
    case record_info(size, ceph_helper_configuration_diff) - 1 of
        N -> record_info(fields, ceph_helper_configuration_diff);
        _ -> no
    end;
get_record_def(cephrados_helper_credentials, N) ->
    case record_info(size, cephrados_helper_credentials) - 1 of
        N -> record_info(fields, cephrados_helper_credentials);
        _ -> no
    end;
get_record_def(cephrados_helper_credentials_diff, N) ->
    case record_info(size, cephrados_helper_credentials_diff) - 1 of
        N -> record_info(fields, cephrados_helper_credentials_diff);
        _ -> no
    end;
get_record_def(cephrados_helper_configuration, N) ->
    case record_info(size, cephrados_helper_configuration) - 1 of
        N -> record_info(fields, cephrados_helper_configuration);
        _ -> no
    end;
get_record_def(cephrados_helper_configuration_diff, N) ->
    case record_info(size, cephrados_helper_configuration_diff) - 1 of
        N -> record_info(fields, cephrados_helper_configuration_diff);
        _ -> no
    end;
get_record_def(glusterfs_helper_credentials, N) ->
    case record_info(size, glusterfs_helper_credentials) - 1 of
        N -> record_info(fields, glusterfs_helper_credentials);
        _ -> no
    end;
get_record_def(glusterfs_helper_credentials_diff, N) ->
    case record_info(size, glusterfs_helper_credentials_diff) - 1 of
        N -> record_info(fields, glusterfs_helper_credentials_diff);
        _ -> no
    end;
get_record_def(glusterfs_helper_configuration, N) ->
    case record_info(size, glusterfs_helper_configuration) - 1 of
        N -> record_info(fields, glusterfs_helper_configuration);
        _ -> no
    end;
get_record_def(glusterfs_helper_configuration_diff, N) ->
    case record_info(size, glusterfs_helper_configuration_diff) - 1 of
        N -> record_info(fields, glusterfs_helper_configuration_diff);
        _ -> no
    end;
get_record_def(http_helper_credentials, N) ->
    case record_info(size, http_helper_credentials) - 1 of
        N -> record_info(fields, http_helper_credentials);
        _ -> no
    end;
get_record_def(http_helper_credentials_diff, N) ->
    case record_info(size, http_helper_credentials_diff) - 1 of
        N -> record_info(fields, http_helper_credentials_diff);
        _ -> no
    end;
get_record_def(http_helper_configuration, N) ->
    case record_info(size, http_helper_configuration) - 1 of
        N -> record_info(fields, http_helper_configuration);
        _ -> no
    end;
get_record_def(http_helper_configuration_diff, N) ->
    case record_info(size, http_helper_configuration_diff) - 1 of
        N -> record_info(fields, http_helper_configuration_diff);
        _ -> no
    end;
get_record_def(nfs_helper_credentials, N) ->
    case record_info(size, nfs_helper_credentials) - 1 of
        N -> record_info(fields, nfs_helper_credentials);
        _ -> no
    end;
get_record_def(nfs_helper_credentials_diff, N) ->
    case record_info(size, nfs_helper_credentials_diff) - 1 of
        N -> record_info(fields, nfs_helper_credentials_diff);
        _ -> no
    end;
get_record_def(nfs_helper_configuration, N) ->
    case record_info(size, nfs_helper_configuration) - 1 of
        N -> record_info(fields, nfs_helper_configuration);
        _ -> no
    end;
get_record_def(nfs_helper_configuration_diff, N) ->
    case record_info(size, nfs_helper_configuration_diff) - 1 of
        N -> record_info(fields, nfs_helper_configuration_diff);
        _ -> no
    end;
get_record_def(nulldevice_helper_credentials, N) ->
    case record_info(size, nulldevice_helper_credentials) - 1 of
        N -> record_info(fields, nulldevice_helper_credentials);
        _ -> no
    end;
get_record_def(nulldevice_helper_credentials_diff, N) ->
    case record_info(size, nulldevice_helper_credentials_diff) - 1 of
        N -> record_info(fields, nulldevice_helper_credentials_diff);
        _ -> no
    end;
get_record_def(nulldevice_helper_configuration, N) ->
    case record_info(size, nulldevice_helper_configuration) - 1 of
        N -> record_info(fields, nulldevice_helper_configuration);
        _ -> no
    end;
get_record_def(nulldevice_helper_configuration_diff, N) ->
    case record_info(size, nulldevice_helper_configuration_diff) - 1 of
        N -> record_info(fields, nulldevice_helper_configuration_diff);
        _ -> no
    end;
get_record_def(posix_helper_credentials, N) ->
    case record_info(size, posix_helper_credentials) - 1 of
        N -> record_info(fields, posix_helper_credentials);
        _ -> no
    end;
get_record_def(posix_helper_credentials_diff, N) ->
    case record_info(size, posix_helper_credentials_diff) - 1 of
        N -> record_info(fields, posix_helper_credentials_diff);
        _ -> no
    end;
get_record_def(posix_helper_configuration, N) ->
    case record_info(size, posix_helper_configuration) - 1 of
        N -> record_info(fields, posix_helper_configuration);
        _ -> no
    end;
get_record_def(posix_helper_configuration_diff, N) ->
    case record_info(size, posix_helper_configuration_diff) - 1 of
        N -> record_info(fields, posix_helper_configuration_diff);
        _ -> no
    end;
get_record_def(s3_helper_credentials, N) ->
    case record_info(size, s3_helper_credentials) - 1 of
        N -> record_info(fields, s3_helper_credentials);
        _ -> no
    end;
get_record_def(s3_helper_credentials_diff, N) ->
    case record_info(size, s3_helper_credentials_diff) - 1 of
        N -> record_info(fields, s3_helper_credentials_diff);
        _ -> no
    end;
get_record_def(s3_helper_configuration, N) ->
    case record_info(size, s3_helper_configuration) - 1 of
        N -> record_info(fields, s3_helper_configuration);
        _ -> no
    end;
get_record_def(s3_helper_configuration_diff, N) ->
    case record_info(size, s3_helper_configuration_diff) - 1 of
        N -> record_info(fields, s3_helper_configuration_diff);
        _ -> no
    end;
get_record_def(swift_helper_credentials, N) ->
    case record_info(size, swift_helper_credentials) - 1 of
        N -> record_info(fields, swift_helper_credentials);
        _ -> no
    end;
get_record_def(swift_helper_credentials_diff, N) ->
    case record_info(size, swift_helper_credentials_diff) - 1 of
        N -> record_info(fields, swift_helper_credentials_diff);
        _ -> no
    end;
get_record_def(swift_helper_configuration, N) ->
    case record_info(size, swift_helper_configuration) - 1 of
        N -> record_info(fields, swift_helper_configuration);
        _ -> no
    end;
get_record_def(swift_helper_configuration_diff, N) ->
    case record_info(size, swift_helper_configuration_diff) - 1 of
        N -> record_info(fields, swift_helper_configuration_diff);
        _ -> no
    end;
get_record_def(webdav_helper_credentials, N) ->
    case record_info(size, webdav_helper_credentials) - 1 of
        N -> record_info(fields, webdav_helper_credentials);
        _ -> no
    end;
get_record_def(webdav_helper_credentials_diff, N) ->
    case record_info(size, webdav_helper_credentials_diff) - 1 of
        N -> record_info(fields, webdav_helper_credentials_diff);
        _ -> no
    end;
get_record_def(webdav_helper_configuration, N) ->
    case record_info(size, webdav_helper_configuration) - 1 of
        N -> record_info(fields, webdav_helper_configuration);
        _ -> no
    end;
get_record_def(webdav_helper_configuration_diff, N) ->
    case record_info(size, webdav_helper_configuration_diff) - 1 of
        N -> record_info(fields, webdav_helper_configuration_diff);
        _ -> no
    end;
get_record_def(xrootd_helper_credentials, N) ->
    case record_info(size, xrootd_helper_credentials) - 1 of
        N -> record_info(fields, xrootd_helper_credentials);
        _ -> no
    end;
get_record_def(xrootd_helper_credentials_diff, N) ->
    case record_info(size, xrootd_helper_credentials_diff) - 1 of
        N -> record_info(fields, xrootd_helper_credentials_diff);
        _ -> no
    end;
get_record_def(xrootd_helper_configuration, N) ->
    case record_info(size, xrootd_helper_configuration) - 1 of
        N -> record_info(fields, xrootd_helper_configuration);
        _ -> no
    end;
get_record_def(xrootd_helper_configuration_diff, N) ->
    case record_info(size, xrootd_helper_configuration_diff) - 1 of
        N -> record_info(fields, xrootd_helper_configuration_diff);
        _ -> no
    end;
get_record_def(_, _) ->
    no.
