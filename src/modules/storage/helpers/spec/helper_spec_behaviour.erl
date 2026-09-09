%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Behaviour for storage helper spec modules - the per type translation between
%%% the typed records of the Onepanel contract and the flat parameter maps
%%% understood by the C++ helper layer.
%%%
%%% Constant properties of a storage type are not part of this behaviour - they
%%% are listed in the storage_type module.
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
%% Reconstructs the typed configuration and credentials records from the flat
%% binary maps, with the confidential credentials redacted.
%% Used to provide storage description to Onepanel (GET operations).
%% @end
%%--------------------------------------------------------------------
-callback describe(helper_spec:t()) ->
    {onedata_storage:helper_configuration(), onedata_storage:helper_credentials()}.


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
