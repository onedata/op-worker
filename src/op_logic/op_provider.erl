%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles op logic operations corresponding to op_transfer model.
%%% @end
%%%-------------------------------------------------------------------
-module(op_provider).
-author("Bartosz Walkowicz").

-include("op_logic.hrl").
-include("global_definitions.hrl").
-include("modules/rtransfer/rtransfer.hrl").

-export([gather_configuration/0]).

% Op logic callbacks
-export([op_logic_plugin/0]).
-export([operation_supported/3]).
-export([get/2]).
-export([authorize/2, data_signature/1]).

-define(to_binaries(__List), [list_to_binary(V) || V <- __List]).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Returns the op logic plugin module that handles model logic.
%% @end
%%--------------------------------------------------------------------
op_logic_plugin() ->
    op_provider.


%%--------------------------------------------------------------------
%% @doc Returns contents of the configuration object.
%% @end
%%--------------------------------------------------------------------
-spec gather_configuration() -> #{binary() := term()}.
gather_configuration() ->
    ProviderId = oneprovider:get_id_or_undefined(),
    Name = case provider_logic:get_name() of
        {ok, N} -> N;
        _ -> undefined
    end,
    Domain = case provider_logic:get_domain() of
        {ok, D} -> D;
        _ -> undefined
    end,
    OnezoneDomain = case ProviderId of
        undefined -> undefined;
        _ -> oneprovider:get_oz_domain()
    end,
    CompOzVersions = application:get_env(?APP_NAME, compatible_oz_versions, []),
    CompOpVersions = application:get_env(?APP_NAME, compatible_op_versions, []),
    CompOcVersions = application:get_env(?APP_NAME, compatible_oc_versions, []),

    #{
        <<"providerId">> => gs_protocol:undefined_to_null(ProviderId),
        <<"name">> => gs_protocol:undefined_to_null(Name),
        <<"domain">> => gs_protocol:undefined_to_null(Domain),
        <<"onezoneDomain">> => gs_protocol:undefined_to_null(OnezoneDomain),
        <<"version">> => oneprovider:get_version(),
        <<"build">> => oneprovider:get_build(),
        <<"rtransferPort">> => ?RTRANSFER_PORT,
        <<"compatibleOnezoneVersions">> => ?to_binaries(CompOzVersions),
        <<"compatibleOneproviderVersions">> => ?to_binaries(CompOpVersions),
        <<"compatibleOneclientVersions">> => ?to_binaries(CompOcVersions)
    }.


%%--------------------------------------------------------------------
%% @doc
%% Determines if given operation is supported based on operation, aspect and
%% scope (entity type is known based on the plugin itself).
%% @end
%%--------------------------------------------------------------------
-spec operation_supported(op_logic:operation(), op_logic:aspect(),
    op_logic:scope()) -> boolean().
operation_supported(get, configuration, private) -> true;

operation_supported(_, _, _) -> false.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves a resource (aspect of entity) based on op logic request and
%% prefetched entity.
%% @end
%%--------------------------------------------------------------------
-spec get(op_logic:req(), op_logic:entity()) -> op_logic:get_result().
get(#op_req{gri = #gri{aspect = configuration}}, _) ->
    {ok, gather_configuration()}.


%%--------------------------------------------------------------------
%% @doc
%% Determines if requesting client is authorized to perform given operation,
%% based on op logic request and prefetched entity.
%% @end
%%--------------------------------------------------------------------
-spec authorize(op_logic:req(), entity_logic:entity()) -> boolean().
authorize(#op_req{operation = get, gri = #gri{aspect = configuration}}, _) ->
    true;

authorize(_, _) ->
    false.


%%--------------------------------------------------------------------
%% @doc
%% Returns data signature for given request.
%% Returns a map with 'required', 'optional' and 'at_least_one' keys.
%% Under each of them, there is a map:
%%      Key => {type_constraint, value_constraint}
%% Which means how value of given Key should be validated.
%% @end
%%--------------------------------------------------------------------
-spec data_signature(op_logic:req()) -> op_validator:data_signature().
data_signature(_) -> #{}.
