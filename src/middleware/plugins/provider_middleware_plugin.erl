%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019-2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles middleware operations (create, get, update, delete)
%%% corresponding to provider aspects such as e.g. instance or configuration.
%%% @end
%%%-------------------------------------------------------------------
-module(provider_middleware_plugin).
-author("Bartosz Walkowicz").

-behaviour(middleware_router).
-behaviour(middleware_handler).

-include("middleware/middleware.hrl").
-include("modules/rtransfer/rtransfer.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([gather_configuration/0]).

%% middleware_router callbacks
-export([resolve_handler/3]).

%% middleware_handler callbacks
-export([data_spec/1, fetch_entity/1, authorize/2, validate/2]).
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
    Version = op_worker:get_release_version(),
    Resolver = compatibility:build_resolver(consistent_hashing:get_all_nodes(), oneprovider:trusted_ca_certs()),
    CompatibilityRegistryRevision = query_compatibility_registry(peek_current_registry_revision, [Resolver]),
    CompOzVersions = query_compatibility_registry(get_compatible_versions, [Resolver, ?ONEPROVIDER, Version, ?ONEZONE]),
    CompOpVersions = query_compatibility_registry(get_compatible_versions, [Resolver, ?ONEPROVIDER, Version, ?ONEPROVIDER]),
    CompOcVersions = query_compatibility_registry(get_compatible_versions, [Resolver, ?ONEPROVIDER, Version, ?ONECLIENT]),

    #{
        <<"providerId">> => utils:undefined_to_null(ProviderId),
        <<"name">> => utils:undefined_to_null(Name),
        <<"domain">> => utils:undefined_to_null(Domain),
        <<"onezoneDomain">> => utils:undefined_to_null(OnezoneDomain),
        <<"version">> => Version,
        <<"build">> => op_worker:get_build_version(),
        <<"rtransferPort">> => ?RTRANSFER_PORT,
        <<"compatibilityRegistryRevision">> => CompatibilityRegistryRevision,
        <<"compatibleOnezoneVersions">> => CompOzVersions,
        <<"compatibleOneproviderVersions">> => CompOpVersions,
        <<"compatibleOneclientVersions">> => CompOcVersions
    }.


%%%===================================================================
%%% middleware_router callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_router} callback resolve_handler/3.
%% @end
%%--------------------------------------------------------------------
-spec resolve_handler(middleware:operation(), gri:aspect(), middleware:scope()) ->
    module() | no_return().
resolve_handler(get, instance, protected) -> ?MODULE;
resolve_handler(get, configuration, public) -> ?MODULE;
resolve_handler(get, test_image, public) -> ?MODULE;
resolve_handler(get, health, public) -> ?MODULE;

resolve_handler(_, _, _) -> throw(?ERROR_NOT_SUPPORTED).


%%%===================================================================
%%% middleware_handler callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback data_spec/1.
%% @end
%%--------------------------------------------------------------------
-spec data_spec(middleware:req()) -> undefined | middleware_sanitizer:data_spec().
data_spec(#op_req{operation = get, gri = #gri{aspect = instance}}) ->
    undefined;
data_spec(#op_req{operation = get, gri = #gri{aspect = configuration}}) ->
    undefined;
data_spec(#op_req{operation = get, gri = #gri{aspect = test_image}}) ->
    undefined;
data_spec(#op_req{operation = get, gri = #gri{aspect = health}}) ->
    undefined.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback fetch_entity/1.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(middleware:req()) ->
    {ok, middleware:versioned_entity()} | errors:error().
fetch_entity(#op_req{gri = #gri{aspect = As, scope = public}}) when
    As =:= configuration;
    As =:= test_image;
    As =:= health
->
    {ok, {undefined, 1}};

fetch_entity(#op_req{auth = ?NOBODY}) ->
    ?ERROR_UNAUTHORIZED;

fetch_entity(#op_req{auth = ?USER(_UserId, SessionId), auth_hint = AuthHint, gri = #gri{
    id = ProviderId,
    aspect = instance,
    scope = protected
}}) ->
    case provider_logic:get_protected_data(SessionId, ProviderId, AuthHint) of
        {ok, #document{value = Provider}} ->
            {ok, {Provider, 1}};
        {error, _} = Error ->
            Error
    end;

fetch_entity(_) ->
    ?ERROR_FORBIDDEN.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback authorize/2.
%% @end
%%--------------------------------------------------------------------
-spec authorize(middleware:req(), middleware:entity()) -> boolean().
authorize(#op_req{operation = get, gri = #gri{aspect = instance}}, _) ->
    % authorization was checked by oz in `fetch_entity`
    true;
authorize(#op_req{operation = get, gri = #gri{aspect = configuration}}, _) ->
    true;
authorize(#op_req{operation = get, gri = #gri{aspect = test_image}}, _) ->
    true;
authorize(#op_req{operation = get, gri = #gri{aspect = health}}, _) ->
    true.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback validate/1.
%% @end
%%--------------------------------------------------------------------
-spec validate(middleware:req(), middleware:entity()) -> ok | no_return().
validate(#op_req{operation = get, gri = #gri{aspect = instance}}, _) ->
    % validation was checked by oz in `fetch_entity`
    ok;
validate(#op_req{operation = get, gri = #gri{aspect = configuration}}, _) ->
    ok;
validate(#op_req{operation = get, gri = #gri{aspect = test_image}}, _) ->
    ok;
validate(#op_req{operation = get, gri = #gri{aspect = health}}, _) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback create/2.
%% @end
%%--------------------------------------------------------------------
-spec create(middleware:req()) -> middleware:create_result().
create(_) ->
    ?ERROR_NOT_SUPPORTED.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(middleware:req(), middleware:entity()) -> middleware:get_result().
get(#op_req{gri = #gri{aspect = instance, scope = protected}}, Provider) ->
    {ok, Provider};
get(#op_req{gri = #gri{aspect = configuration}}, _) ->
    {ok, value, gather_configuration()};
get(#op_req{gri = #gri{aspect = test_image}}, _) ->
    % Dummy image in png format. Used by gui to check connectivity.
    {ok, value, {binary, <<
        137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82, 0, 0,
        0, 1, 0, 0, 0, 1, 1, 3, 0, 0, 0, 37, 219, 86, 202, 0, 0, 0, 6, 80,
        76, 84, 69, 0, 0, 0, 255, 255, 255, 165, 217, 159, 221, 0, 0, 0, 9,
        112, 72, 89, 115, 0, 0, 14, 196, 0, 0, 14, 196, 1, 149, 43, 14, 27,
        0, 0, 0, 10, 73, 68, 65, 84, 8, 153, 99, 96, 0, 0, 0, 2, 0, 1, 244,
        113, 100, 166, 0, 0, 0, 0, 73, 69, 78, 68, 174, 66, 96, 130
    >>}};
get(#op_req{gri = #gri{aspect = health}}, _) ->
    case node_manager:is_cluster_healthy() of
        true -> {ok, value, #{<<"status">> => <<"healthy">>}};
        false -> throw(?ERROR_INTERNAL_SERVER_ERROR)
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback update/1.
%% @end
%%--------------------------------------------------------------------
-spec update(middleware:req()) -> middleware:update_result().
update(_) ->
    ?ERROR_NOT_SUPPORTED.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(middleware:req()) -> middleware:delete_result().
delete(_) ->
    ?ERROR_NOT_SUPPORTED.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec query_compatibility_registry(Fun :: atom(), Args :: [term()]) -> term().
query_compatibility_registry(Fun, Args) ->
    Module = compatibility,
    case apply(Module, Fun, Args) of
        {ok, SuccessfulResult} ->
            SuccessfulResult;
        {error, _} = Error ->
            ?debug("Error querying registry - ~w:~w(~w)~nError was: ~tp", [Module, Fun, Args, Error]),
            <<"unknown">>
    end.
