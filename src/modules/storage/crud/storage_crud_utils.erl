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
    normalize_qos_parameters/1,
    pretty_print_spec/1,
    verify_storage_path_type/1,
    verify_credentials/1,
    verify_configuration/4,
    verify_s3_region/1,
    verify_s3_region_update/2,
    run_diagnostics/3
]).

-define(GLOBAL_AWS_S3_HOST, <<"s3.amazonaws.com">>).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Stores as numbers the QoS parameters that an expression reads as numbers,
%% see qos_expression:normalize_storage_parameters/1. An update without QoS
%% parameters keeps them undefined, as they replace all the previous ones.
%% @end
%%--------------------------------------------------------------------
-spec normalize_qos_parameters(onedata_storage:create_spec() | onedata_storage:update_spec()) ->
    onedata_storage:create_spec() | onedata_storage:update_spec().
normalize_qos_parameters(Spec = #storage_create_spec{qos_parameters = QosParameters}) ->
    Spec#storage_create_spec{qos_parameters = qos_expression:normalize_storage_parameters(QosParameters)};
normalize_qos_parameters(Spec = #storage_update_spec{qos_parameters = undefined}) ->
    Spec;
normalize_qos_parameters(Spec = #storage_update_spec{qos_parameters = QosParameters}) ->
    Spec#storage_update_spec{qos_parameters = qos_expression:normalize_storage_parameters(QosParameters)}.


-spec pretty_print_spec(onedata_storage:create_spec() | onedata_storage:update_spec()) ->
    io_lib:chars().
pretty_print_spec(Spec) ->
    RedactedSpec = redact_confidential_data(Spec),
    io_lib_pretty:print(RedactedSpec, fun get_record_def/2).


%%--------------------------------------------------------------------
%% @doc
%% Checks the storage path type against the domain of the storage type.
%% Done on storage creation only - the storage path type is not part of any
%% helper configuration diff, so an update can never change it.
%% @end
%%--------------------------------------------------------------------
-spec verify_storage_path_type(helper_spec:t()) -> ok | no_return().
verify_storage_path_type(HelperSpec) ->
    SupportedStoragePathTypes = storage_type:list_supported_storage_path_types(
        helper_spec:get_name(HelperSpec)
    ),
    case lists:member(helper_spec:get_storage_path_type(HelperSpec), SupportedStoragePathTypes) of
        true ->
            ok;
        false ->
            throw(?ERR_BAD_VALUE_NOT_ALLOWED(
                ?err_ctx(), <<"storagePathType">>, SupportedStoragePathTypes
            ))
    end.


%%--------------------------------------------------------------------
%% @doc
%% Checks the storage's own credentials the way every use of the storage checks
%% them before it makes a helper (see helper_spec:build_helper_params/2), so that
%% credentials which would fail every use are refused before anything is
%% written. The diagnostics don't cover it, as they skip some storage types.
%% @end
%%--------------------------------------------------------------------
-spec verify_credentials(helper_spec:t()) -> ok | no_return().
verify_credentials(HelperSpec) ->
    case helper_spec:validate_credentials(HelperSpec, helper_spec:get_credentials(HelperSpec)) of
        ok ->
            ok;
        {error, {missing_field, Field}} ->
            throw(?ERR_MISSING_REQUIRED_VALUE(?err_ctx(), Field));
        {error, {unexpected_field, Field}} ->
            throw(?ERR_BAD_DATA(?err_ctx(), Field, undefined));
        {error, {invalid_field_value, Field, _Value}} ->
            % the value stays out of the error, as it may be a secret
            throw(?ERR_BAD_DATA(?err_ctx(), Field, undefined))
    end.


-spec verify_configuration(storage:id() | storage:name(), storage:readonly(), storage:imported(), helper_spec:t()) ->
    ok | no_return().
verify_configuration(IdOrName, Readonly, Imported, HelperSpec) ->
    sanitize_readonly_option(Readonly, Imported, IdOrName),
    check_helper_against_readonly_option(Readonly, HelperSpec),
    check_helper_against_imported_option(Imported, HelperSpec).


%%--------------------------------------------------------------------
%% @doc
%% Requires the region of an S3 storage at the global AWS endpoint,
%% s3.amazonaws.com on any port. Without the region, each time the helper is
%% created it asks AWS for the bucket's region before its first request; given
%% one, it goes straight to that region's endpoint (but for us-east-1, which it
%% takes for no region). An endpoint is global by its host alone, as the helper
%% tells it in S3Helper::isGlobalAwsEndpoint.
%% @end
%%--------------------------------------------------------------------
-spec verify_s3_region(helper_spec:t()) -> ok | no_return().
verify_s3_region(HelperSpec) ->
    case helper_spec:get_name(HelperSpec) of
        ?S3_HELPER_NAME -> verify_s3_region_in_configuration(helper_spec:get_configuration(HelperSpec));
        _ -> ok
    end.


-spec verify_s3_region_update(helper_spec:t(), helper_spec:t()) -> ok | no_return().
verify_s3_region_update(PrevHelperSpec, NewHelperSpec) ->
    case get_hostname_and_region(PrevHelperSpec) == get_hostname_and_region(NewHelperSpec) of
        true -> ok;
        false -> verify_s3_region(NewHelperSpec)
    end.


-spec run_diagnostics(helper_spec:t(), luma:feed(), storage_detector:diagnostics_mode()) ->
    ok | no_return().
run_diagnostics(HelperSpec, LumaFeed, DiagnosticsMode) ->
    case storage_detector:run_diagnostics(all_nodes, HelperSpec, LumaFeed, DiagnosticsMode) of
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
            throw(?ERR_STORAGE_IMPORT_NOT_SUPPORTED(
                ?err_ctx(), HelperName,
                storage_type:list_types_with_capability(object_storage)
            ));
        true ->
            ok
    end.


%% @private
-spec verify_s3_region_in_configuration(helper_spec:configuration()) -> ok | no_return().
verify_s3_region_in_configuration(Configuration = #{<<"scheme">> := Scheme, <<"hostname">> := Hostname}) ->
    case is_global_aws_endpoint(Scheme, Hostname) of
        false ->
            ok;
        true ->
            case maps:get(<<"region">>, Configuration, undefined) of
                undefined -> throw(?ERR_MISSING_REQUIRED_VALUE(?err_ctx(), <<"region">>));
                <<>> -> throw(?ERR_BAD_VALUE_EMPTY(?err_ctx(), <<"region">>));
                _ -> ok
            end
    end.


%% @private
%% The hostname holds no scheme, and uri_string reads a host and a port without
%% one as a scheme and a path, so the scheme is put back in front.
-spec is_global_aws_endpoint(binary(), binary()) -> boolean().
is_global_aws_endpoint(Scheme, Hostname) ->
    case uri_string:parse(<<Scheme/binary, "://", Hostname/binary>>) of
        #{host := ?GLOBAL_AWS_S3_HOST} -> true;
        _ -> false
    end.


%% @private
-spec get_hostname_and_region(helper_spec:t()) -> helper_spec:configuration().
get_hostname_and_region(HelperSpec) ->
    maps:with([<<"hostname">>, <<"region">>], helper_spec:get_configuration(HelperSpec)).


%% @private
-spec redact_confidential_data(onedata_storage:create_spec() | onedata_storage:update_spec()) ->
    onedata_storage:create_spec() | onedata_storage:update_spec().
redact_confidential_data(Spec = #storage_create_spec{type = Type, luma = Luma, credentials = Credentials}) ->
    Spec#storage_create_spec{
        luma = redact_luma_api_key(Luma),
        credentials = helper_spec:redact_confidential_credentials(Type, Credentials)
    };
redact_confidential_data(Spec = #storage_update_spec{type = Type, luma = Luma, credentials = CredentialsDiff}) ->
    Spec#storage_update_spec{
        luma = redact_luma_api_key(Luma),
        credentials = redact_confidential_credentials_diff(Type, CredentialsDiff)
    }.


%% @private
-spec redact_luma_api_key(undefined | onedata_storage:luma_spec() | onedata_storage:luma_spec_diff()) ->
    undefined | onedata_storage:luma_spec() | onedata_storage:luma_spec_diff().
redact_luma_api_key(undefined) ->
    undefined;
redact_luma_api_key(LumaSpec = #luma_spec{}) ->
    helper_spec_utils:redact_record_fields_if_defined(LumaSpec, [#luma_spec.api_key]);
redact_luma_api_key(LumaSpecDiff = #luma_spec_diff{}) ->
    helper_spec_utils:redact_record_fields_if_defined(LumaSpecDiff, [#luma_spec_diff.api_key]).


%% @private
-spec redact_confidential_credentials_diff(onedata_storage:type(), undefined | onedata_storage:helper_credentials_diff()) ->
    undefined | onedata_storage:helper_credentials_diff().
redact_confidential_credentials_diff(_Type, undefined) ->
    undefined;
redact_confidential_credentials_diff(Type, CredentialsDiff) ->
    helper_spec:redact_confidential_credentials_diff(Type, CredentialsDiff).


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
get_record_def(luma_spec_diff, N) ->
    case record_info(size, luma_spec_diff) - 1 of
        N -> record_info(fields, luma_spec_diff);
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
