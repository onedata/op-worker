%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Behaviour for storage helper spec modules.
%%% @end
%%%-------------------------------------------------------------------
-module(helper_spec_behaviour).
-author("Bartosz Walkowicz").

-include("modules/storage/helpers/helpers.hrl").


%%%===================================================================
%%% Callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Creates a new helper_spec from storage_create request.
%% Converts typed records (configuration and credentials) into binary maps
%% suitable for the C++ helper layer.
%% @end
%%--------------------------------------------------------------------
-callback build(onedata_storage:create_spec()) ->
    helper_spec:t().


-callback validate_credentials(helper_spec:credentials()) ->
    ok | {error, Reason :: term()}.


%%--------------------------------------------------------------------
%% @doc
%% Builds a diff map of configuration changes from update specification.
%% @end
%%--------------------------------------------------------------------
-callback build_configuration_diff(helper_spec:t(), onedata_storage:update_spec()) ->
    helper_spec:configuration().


%%--------------------------------------------------------------------
%% @doc
%% Builds a diff map of credentials changes from update specification.
%% @end
%%--------------------------------------------------------------------
-callback build_credentials_diff(helper_spec:t(), onedata_storage:update_spec()) ->
    helper_spec:credentials().


%%--------------------------------------------------------------------
%% @doc
%% Reconstructs typed records from helper_spec binary maps.
%% Used to provide storage description to Onepanel (GET operations).
%% @end
%%--------------------------------------------------------------------
-callback describe(helper_spec:t()) -> helper_spec:description().


-callback is_posix_compatible() -> boolean().


-callback is_object_storage() -> boolean().


-callback is_rename_supported() -> boolean().


-callback is_nfs4_acl_supported() -> boolean().


-callback is_oauth2_supported() -> boolean().


-callback is_storage_access_type_supported(helper_spec:access_type()) -> boolean().


-callback is_auto_import_supported(helper_spec:t()) -> boolean().


-callback is_file_registration_supported(helper_spec:t()) -> boolean().


-callback is_getting_size_supported(helper_spec:t()) -> boolean().


%%--------------------------------------------------------------------
%% @doc
%% Returns the block size used by the storage.
%% Returns undefined for non-object storage types.
%% @end
%%--------------------------------------------------------------------
-callback get_block_size(helper_spec:t()) -> non_neg_integer() | undefined.


%%--------------------------------------------------------------------
%% @doc
%% Redacts confidential fields in credentials record for security reasons.
%% Used before logging or displaying credentials.
%% @end
%%--------------------------------------------------------------------
-callback redact_confidential_credentials(onedata_storage:helper_credentials()) ->
    onedata_storage:helper_credentials().


%%--------------------------------------------------------------------
%% @doc
%% Redacts confidential fields in credentials diff record for security reasons.
%% Used before logging or displaying credentials diff.
%% @end
%%--------------------------------------------------------------------
-callback redact_confidential_credentials_diff(onedata_storage:helper_credentials_diff()) ->
    onedata_storage:helper_credentials_diff().
