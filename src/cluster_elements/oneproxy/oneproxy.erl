%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2014 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Port driver for oneproxy module.
%%% @end
%%%-------------------------------------------------------------------
-module(oneproxy).
-author("Rafal Slota").

-include("registered_names.hrl").
-include("cluster_elements/oneproxy/oneproxy.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("public_key/include/public_key.hrl").

%% API
-export([start/3, start_proxy/3, start_rproxy/4, start_rproxy/5, main_loop/2,
    get_session/2, get_local_port/1, get_der_certs_dir/0, ca_crl_to_der/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Gets session data for given session ID. If session does not exists, returns empty data,
%% although caller shall always assume that the session exists and crash otherwise due to malformed data.
%% @end
%%--------------------------------------------------------------------
-spec get_session(OneProxyNameOrPid :: atom() | pid(), SessionId :: binary()) -> {ok, Data :: binary()} | {error, Reason :: any()}.
get_session(OneProxyNameOrPid, SessionId) ->
    exec(OneProxyNameOrPid, <<"get_session">>, [<<(utils:ensure_binary(SessionId))/binary>>]).


%%--------------------------------------------------------------------
%% @doc
%% Maps TLS endpoint port to local TCP endpoint port.
%% @todo Solve potential conflict with user-defined ports.
%% @end
%%--------------------------------------------------------------------
-spec get_local_port(Port :: intet:port()) -> LocalPort :: non_neg_integer().
get_local_port(443) ->
    12001;
get_local_port(5555) ->
    12002;
get_local_port(8443) ->
    12003;
get_local_port(8877) ->
    12005;
get_local_port(Port) ->
    20000 + Port.

%%--------------------------------------------------------------------
%% @doc
%% Starts oneproxy in proxy mode. This function either does not return or throws exception.
%% @end
%%--------------------------------------------------------------------
-spec start_proxy(ListenerPort :: non_neg_integer(), CertFile :: string() | binary(),
    VerifyType :: verify_peer | verify_none) -> no_return().
start_proxy(ListenerPort, CertFile, VerifyType) ->
    start("proxy", ListenerPort, [utils:ensure_list(CertFile),
        utils:ensure_list(VerifyType)]).

%%--------------------------------------------------------------------
%% @doc
%% Starts oneproxy in reverse proxy mode with HTTP-based authentication
%% delegation. This function either does not return or throws exception.
%% @end
%%--------------------------------------------------------------------
-spec start_rproxy(ListenerPort :: non_neg_integer(), ForwardPort :: non_neg_integer(),
    CertFile :: string() | binary(), VerifyType :: verify_peer | verify_none) -> no_return().
start_rproxy(ListenerPort, ForwardPort, CertFile, VerifyType) ->
    start_rproxy(ListenerPort, ForwardPort, CertFile, VerifyType, http).

%%--------------------------------------------------------------------
%% @doc
%% Starts oneproxy in reverse proxy mode. This function either does not return or throws exception.
%% @end
%%--------------------------------------------------------------------
-spec start_rproxy(ListenerPort :: non_neg_integer(), ForwardPort :: non_neg_integer(),
    CertFile :: string() | binary(), VerifyType :: verify_peer | verify_none,
    Http :: http | no_http) -> no_return().
start_rproxy(ListenerPort, ForwardPort, CertFile, VerifyType, Http) ->
    start("reverse_proxy", ListenerPort, ["127.0.0.1",
        integer_to_list(ForwardPort), utils:ensure_list(CertFile),
        utils:ensure_list(VerifyType), utils:ensure_list(Http)]).

%%--------------------------------------------------------------------
%% @doc
%% Returns directory (path) used by oneproxy to save DER CAs and CRLs.
%% @end
%%--------------------------------------------------------------------
-spec get_der_certs_dir() -> string().
get_der_certs_dir() ->
    {ok, CertDir} = application:get_env(?APP_NAME, ca_dir),
    filename:join(CertDir, ?DER_CERTS_DIR).

%%--------------------------------------------------------------------
%% @doc
%% Save all CAs and CRLs from GSI state to given directory in DER format.
%% @end
%%--------------------------------------------------------------------
-spec ca_crl_to_der(Dir :: string()) -> ok | {error, Reason :: any()}.
ca_crl_to_der(Dir) ->
    case ets:info(gsi_state) of
        undefined ->
            ?warning("Cannot save raw CA and CRL certificates without active GSI Handler"),
            {error, no_gsi};
        _ ->
            try
                case file:make_dir(Dir) of
                    ok -> ok;
                    {error, eexist} -> ok;
                    {error, Reason} ->
                        ?error("Cannot create dir ~p due to ~p", [Dir, Reason])
                end,
                CAs = [{DER, public_key:der_decode('Certificate', DER)} || [DER] <- ets:match(gsi_state, {{ca, '_'}, '$1', '_'})],
                CRLs = [{DER, public_key:der_decode('CertificateList', DER)} || [DER] <- ets:match(gsi_state, {{crl, '_'}, '$1', '_'})],

                lists:foreach(
                    fun({DER, #'Certificate'{tbsCertificate = #'TBSCertificate'{subject = Subject}}}) ->
                        FN0 = base64:encode(crypto:hash(md5, term_to_binary(Subject))),
                        FN = re:replace(FN0, "/", "_", [{return, list}]),
                        file:write_file(filename:join(Dir, utils:ensure_list(FN) ++ ".crt"), DER)
                    end, CAs),

                lists:foreach(
                    fun({DER, #'CertificateList'{tbsCertList = #'TBSCertList'{issuer = Issuer}}}) ->
                        FN0 = base64:encode(crypto:hash(md5, term_to_binary(Issuer))),
                        FN = re:replace(FN0, "/", "_", [{return, list}]),
                        file:write_file(filename:join(Dir, utils:ensure_list(FN) ++ ".crl"), DER)
                    end, CRLs),
                ok
            catch
                Type:Reason1 ->
                    ?error_stacktrace("Failed to save DER CAs and CRLs due to: ~p:~p", [Type, Reason1]),
                    {error, Reason1}
            end
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% oneproxy port dirver's main loop.
%% @end
%%--------------------------------------------------------------------
-spec exec(OneProxy :: pid() | atom(), CMD :: binary(), Args :: [binary()]) -> term().
%% ====================================================================
exec(OneProxy, CMD, Args) when is_atom(OneProxy) ->
    exec(whereis(OneProxy), CMD, Args);
exec(OneProxyPid, CMD, Args) when is_pid(OneProxyPid) ->
    Id = make_ref(),
    OneProxyPid ! {{self(), Id}, {command, CMD, Args}},
    receive
        {{OneProxyPid, Id}, Response} -> Response
    after timer:seconds(5) ->
        {error, timeout}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts oneproxy. This function either does not return or throws exception.
%% @end
%%--------------------------------------------------------------------
-spec start(Mode :: string(), ListenerPort :: non_neg_integer(), Args :: list()) -> no_return().
start(Mode, ListenerPort, Args) ->
    {ok, CWD} = file:get_cwd(),
    ExecPath = os:find_executable("oneproxy", filename:join(CWD, "c_lib")),

    {ok, CADir} = application:get_env(?APP_NAME, ca_dir),

    %% Try to load certs before starting proxy
    catch ca_crl_to_der(get_der_certs_dir()),

    Port = open_port({spawn_executable, ExecPath}, [
        {line, 1024 * 1024}, binary,
        {args, [Mode, integer_to_list(ListenerPort)] ++ Args ++ [filename:join(CADir, ?DER_CERTS_DIR)]}
    ]),
    try
        timer:send_after(timer:seconds(0), reload_certs),
        timer:send_after(timer:seconds(10), heartbeat),
        main_loop(Port, #oneproxy_state{timeout = timer:seconds(1), endpoint = ListenerPort})
    catch
        Type:Reason ->
            ?error_stacktrace("oneproxy port error ~p:~p", [Type, Reason]),
            catch port_close(Port),
            timer:sleep(timer:seconds(1)),
            ?MODULE:start(Mode, ListenerPort, Args)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% oneproxy port dirver's main loop.
%% @end
%%--------------------------------------------------------------------
-spec main_loop(Port :: term(), State :: #oneproxy_state{}) -> no_return().
main_loop(Port, #oneproxy_state{timeout = Timeout, endpoint = EnpointPort} = State) ->
    NewState =
        receive
        %% Handle oneproxy logs
            {Port, {data, {eol, <<?LOG_DEBUG_PREFIX, Log/binary>>}}} ->
                ?debug("[ oneproxy ~p ] ~s", [EnpointPort, utils:ensure_list(Log)]),
                State;
            {Port, {data, {eol, <<?LOG_INFO_PREFIX, Log/binary>>}}} ->
                ?info("[ oneproxy ~p ] ~s", [EnpointPort, utils:ensure_list(Log)]),
                State;
            {Port, {data, {eol, <<?LOG_WARNING_PREFIX, Log/binary>>}}} ->
                ?warning("[ oneproxy ~p ] ~s", [EnpointPort, utils:ensure_list(Log)]),
                State;
            {Port, {data, {eol, <<?LOG_ERROR_PREFIX, Log/binary>>}}} ->
                ?error("[ oneproxy ~p ] ~s", [EnpointPort, utils:ensure_list(Log)]),
                State;
            {'EXIT', Port, Reason} ->
                ?error("oneproxy port terminated due to: ~p", [Reason]),
                error({terminated, Reason});

        %% Checks if port is still alive
            heartbeat ->
                timer:send_after(timer:seconds(3), heartbeat),
                port_command(Port, <<"heartbeat\n">>),
                State;
        %% Reloads GSI CAs and CRLs in oneproxy port
            reload_certs ->
                case ca_crl_to_der(get_der_certs_dir()) of
                    ok ->
                        port_command(Port, <<"reload_certs\n">>),
                        timer:send_after(timer:minutes(5), reload_certs),
                        State;
                    {error, Reason1} ->
                        ?warning("Could not reload certificates for oneproxy due to: ~p", [Reason1]),
                        timer:send_after(500, reload_certs),
                        State
                end;
        %% Executes given command on oneproxy and replays with {ok, Response} or {error, Reason}
            {{Pid, Id}, {command, CMD, Args}} ->
                BinPid = base64:encode(term_to_binary(Pid)),
                PidSize = size(BinPid),
                ArgsBin = utils:binary_join(Args, <<" ">>),
                FullCmd = <<CMD/binary, " ", BinPid/binary, " ", ArgsBin/binary, "\n">>,
                port_command(Port, FullCmd),
                receive
                    {Port, {data, {eol, <<BinPid:PidSize/binary, " ", SessionData/binary>>}}} ->
                        Pid ! {{self(), Id}, {ok, SessionData}}
                after timer:seconds(5) ->
                    Pid ! {{self(), Id}, {error, port_timeout}}
                end,
                State
        after Timeout ->
            State
        end,

    ?MODULE:main_loop(Port, NewState).
