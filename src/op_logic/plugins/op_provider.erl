%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles op logic operations (create, get, update, delete)
%%% corresponding to provider aspects such as:
%%% - configuration.
%%% @end
%%%-------------------------------------------------------------------
-module(op_provider).
-author("Bartosz Walkowicz").

-behaviour(op_logic_behaviour).

-include("op_logic.hrl").
-include("global_definitions.hrl").
-include("modules/rtransfer/rtransfer.hrl").
-include_lib("ctool/include/api_errors.hrl").
-include_lib("ctool/include/onedata.hrl").

-export([gather_configuration/0]).

% op logic callbacks
-export([op_logic_plugin/0]).
-export([
    operation_supported/3,
    data_spec/1,
    fetch_entity/1,
    exists/2,
    authorize/2,
    validate/2
]).
-export([create/1, get/2, update/1, delete/1]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Returns contents of the configuration object.
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
    Version = oneprovider:get_version(),
    {ok, CompOzVersions} = compatibility:get_compatible_versions(?ONEPROVIDER, Version, ?ONEZONE),
    {ok, CompOpVersions} = compatibility:get_compatible_versions(?ONEPROVIDER, Version, ?ONEPROVIDER),
    {ok, CompOcVersions} = compatibility:get_compatible_versions(?ONEPROVIDER, Version, ?ONECLIENT),

    #{
        <<"providerId">> => gs_protocol:undefined_to_null(ProviderId),
        <<"name">> => gs_protocol:undefined_to_null(Name),
        <<"domain">> => gs_protocol:undefined_to_null(Domain),
        <<"onezoneDomain">> => gs_protocol:undefined_to_null(OnezoneDomain),
        <<"version">> => Version,
        <<"build">> => oneprovider:get_build(),
        <<"rtransferPort">> => ?RTRANSFER_PORT,
        <<"compatibleOnezoneVersions">> => CompOzVersions,
        <<"compatibleOneproviderVersions">> => CompOpVersions,
        <<"compatibleOneclientVersions">> => CompOcVersions
    }.


%%--------------------------------------------------------------------
%% @doc
%% Returns the op logic plugin module that handles model logic.
%% @end
%%--------------------------------------------------------------------
op_logic_plugin() ->
    op_provider.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback operation_supported/3.
%% @end
%%--------------------------------------------------------------------
-spec operation_supported(op_logic:operation(), op_logic:aspect(),
    op_logic:scope()) -> boolean().
operation_supported(get, configuration, private) -> true;
operation_supported(get, test_image, private) -> true;

operation_supported(_, _, _) -> false.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback data_spec/1.
%% @end
%%--------------------------------------------------------------------
-spec data_spec(op_logic:req()) -> undefined | op_sanitizer:data_spec().
data_spec(#op_req{operation = get, gri = #gri{aspect = configuration}}) ->
    undefined;
data_spec(#op_req{operation = get, gri = #gri{aspect = test_image}}) ->
    undefined.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback fetch_entity/1.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(op_logic:req()) ->
    {ok, op_logic:versioned_entity()} | op_logic:error().
fetch_entity(_) ->
    {ok, {undefined, 1}}.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback exists/2.
%% @end
%%--------------------------------------------------------------------
-spec exists(op_logic:req(), op_logic:entity()) -> boolean().
exists(_, _) ->
    true.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback authorize/2.
%% @end
%%--------------------------------------------------------------------
-spec authorize(op_logic:req(), op_logic:entity()) -> boolean().
authorize(#op_req{operation = get, gri = #gri{aspect = configuration}}, _) ->
    true;
authorize(#op_req{operation = get, gri = #gri{aspect = test_image}}, _) ->
    true.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback validate/1.
%% @end
%%--------------------------------------------------------------------
-spec validate(op_logic:req(), op_logic:entity()) -> ok | no_return().
validate(#op_req{operation = get, gri = #gri{aspect = configuration}}, _) ->
    ok;
validate(#op_req{operation = get, gri = #gri{aspect = test_image}}, _) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback create/2.
%% @end
%%--------------------------------------------------------------------
-spec create(op_logic:req()) -> op_logic:create_result().
create(_) ->
    ?ERROR_NOT_SUPPORTED.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(op_logic:req(), op_logic:entity()) -> op_logic:get_result().
get(#op_req{gri = #gri{aspect = configuration}}, _) ->
    {ok, gather_configuration()};
get(#op_req{gri = #gri{aspect = test_image}}, _) ->
    % Dummy image in png format. Used by gui to check connectivity.
    {ok, {binary, <<
        137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82, 0, 0,
        0, 1, 0, 0, 0, 1, 1, 3, 0, 0, 0, 37, 219, 86, 202, 0, 0, 0, 6, 80,
        76, 84, 69, 0, 0, 0, 255, 255, 255, 165, 217, 159, 221, 0, 0, 0, 9,
        112, 72, 89, 115, 0, 0, 14, 196, 0, 0, 14, 196, 1, 149, 43, 14, 27,
        0, 0, 0, 10, 73, 68, 65, 84, 8, 153, 99, 96, 0, 0, 0, 2, 0, 1, 244,
        113, 100, 166, 0, 0, 0, 0, 73, 69, 78, 68, 174, 66, 96, 130
    >>}}.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback update/1.
%% @end
%%--------------------------------------------------------------------
-spec update(op_logic:req()) -> op_logic:update_result().
update(_) ->
    ?ERROR_NOT_SUPPORTED.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(op_logic:req()) -> op_logic:delete_result().
delete(_) ->
    ?ERROR_NOT_SUPPORTED.