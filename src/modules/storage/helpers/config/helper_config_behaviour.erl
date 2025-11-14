%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Behaviour for storage helper configuration modules.
%%% @end
%%%-------------------------------------------------------------------
-module(helper_config_behaviour).
-author("Bartosz Walkowicz").

-include("storage/common.hrl").
-include("modules/storage/helpers/helpers.hrl").


%%%===================================================================
%%% Callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Creates a new helper_config from storage_create request.
%% Converts typed records (configuration and credentials) into binary maps
%% suitable for the C++ helper layer.
%% @end
%%--------------------------------------------------------------------
-callback build(onedata_storage:create_spec()) ->
    helper_config:t().


-callback validate_user_ctx(helper_config:user_ctx()) ->
    ok | {error, Reason :: term()}.


%%--------------------------------------------------------------------
%% @doc
%% Builds a diff map of args changes from update specification.
%% @end
%%--------------------------------------------------------------------
-callback build_args_diff(helper_config:t(), onedata_storage:update_spec()) ->
    helper_config:args().


%%--------------------------------------------------------------------
%% @doc
%% Builds a diff map of admin_ctx changes from update specification.
%% @end
%%--------------------------------------------------------------------
-callback build_admin_ctx_diff(helper_config:t(), onedata_storage:update_spec()) ->
    helper_config:user_ctx().


%%--------------------------------------------------------------------
%% @doc
%% Reconstructs typed records from helper_config binary maps.
%% Used to provide storage description to Onepanel (GET operations).
%% @end
%%--------------------------------------------------------------------
-callback describe(helper_config:t()) -> helper_config:description().


-callback is_posix_compatible() -> boolean().


-callback is_object() -> boolean().


-callback is_rename_supported() -> boolean().


-callback is_nfs4_acl_supported() -> boolean().


-callback supports_storage_access_type(helper_config:access_type()) -> boolean().


-callback is_auto_import_supported(helper_config:t()) -> boolean().


-callback is_file_registration_supported(helper_config:t()) -> boolean().


-callback is_getting_size_supported(helper_config:t()) -> boolean().


%%--------------------------------------------------------------------
%% @doc
%% Returns the block size used by the storage.
%% Returns undefined for non-object storage types.
%% @end
%%--------------------------------------------------------------------
-callback get_block_size(helper_config:t()) -> non_neg_integer() | undefined.
