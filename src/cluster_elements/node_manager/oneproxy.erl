%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: @todo: write me!
%% @end
%% ===================================================================
-module(oneproxy).
-author("Rafal Slota").

-include("registered_names.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("public_key/include/public_key.hrl").

-define(DER_CERTS_DIR, "der_certs").
-record(oneproxy_state, {timeout = timer:minutes(1), endpoint}).

%% API
-export([start/4, main_loop/2, get_session/2, get_local_port/1]).


get_session(OneProxyPid, SessionId) ->
    exec(OneProxyPid, <<"get_session ", (vcn_utils:ensure_binary(SessionId))/binary>>).

get_local_port(443) ->
    12001;
get_local_port(5555) ->
    12002;
get_local_port(8443) ->
    12003;
get_local_port(Port) ->
    20000 + Port.

start(ListenerPort, ForwardPort, CertFile, VerifyType) ->
    {ok, CWD} = file:get_cwd(),
    ExecPath = os:find_executable("oneproxy", filename:join(CWD, "c_lib")),

    {ok, CADir1} = application:get_env(?APP_Name, ca_dir),
    CADir = atom_to_list(CADir1),

    %% Try to load certs before starting proxy
    catch ca_crl_to_der(get_der_certs_dir()),

    Port = open_port({spawn_executable, ExecPath}, [
        {line, 1024 * 1024}, binary,
        {args, [integer_to_list(ListenerPort), "127.0.0.1", integer_to_list(ForwardPort), CertFile, vcn_utils:ensure_list(VerifyType), filename:join(CADir, ?DER_CERTS_DIR)]}
    ]),
    try
        timer:send_after(timer:seconds(0), {self(), reload_certs}),
        main_loop(Port, #oneproxy_state{timeout = timer:seconds(1), endpoint = ListenerPort})
    catch
        Type:Reason ->
            ?error_stacktrace("oneproxy port error ~p:~p", [Type, Reason]),
            catch port_close(Port),
            timer:sleep(timer:minutes(1)),
            ?MODULE:start(ListenerPort, ForwardPort, CertFile, VerifyType)
    end.


main_loop(Port, #oneproxy_state{timeout = Timeout, endpoint = EnpointPort} = State) ->
    NewState = receive
        {Port, {data, {eol, <<"[ DEBUG ] ", Log/binary>>}}} ->
            ?debug("[ oneproxy ~p ] ~s", [EnpointPort, vcn_utils:ensure_list(Log)]),
            State;
        {Port, {data, {eol, <<"[ INFO ] ", Log/binary>>}}} ->
            ?info("[ oneproxy ~p ] ~s", [EnpointPort, vcn_utils:ensure_list(Log)]),
            State;
        {Port, {data, {eol, <<"[ WARNING ] ", Log/binary>>}}} ->
            ?warning("[ oneproxy ~p ] ~s", [EnpointPort, vcn_utils:ensure_list(Log)]),
            State;
        {Port, {data, {eol, <<"[ ERROR ] ", Log/binary>>}}} ->
            ?error("[ oneproxy ~p ] ~s", [EnpointPort, vcn_utils:ensure_list(Log)]),
            State;
        {'EXIT', Port, Reason} ->
            ?error("oneproxy port terminated due to: ~p", [Reason]),
            error({terminated, Reason});
        {_Pid, reload_certs} ->
            case ca_crl_to_der(get_der_certs_dir()) of
                ok ->
                    port_command(Port, <<"reload_certs\n">>),
                    timer:send_after(timer:minutes(5), {self(), reload_certs}),
                    State;
                {error, _} ->
                    timer:send_after(timer:seconds(2), {self(), reload_certs}),
                    State
            end;
        {Pid, {command, CMD}} ->
            port_command(Port, <<CMD/binary, "\n">>),
            receive
                {Port, {data, {eol, SessionData}}} ->
                    Pid ! {ok, SessionData}
            after 500 ->
                Pid ! {error, port_timeout}
            end,
            State
    after Timeout ->
        State
    end,

    ?MODULE:main_loop(Port, NewState).


get_der_certs_dir() ->
    {ok, CertDir1} = application:get_env(?APP_Name, ca_dir),
    CertDir = atom_to_list(CertDir1),
    filename:join(CertDir, ?DER_CERTS_DIR).


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
                        FN = re:replace(FN0,"/","_",[{return,list}]),
                        file:write_file(filename:join(Dir, vcn_utils:ensure_list(FN) ++ ".crt"), DER)
                    end, CAs),

                lists:foreach(
                    fun({DER, #'CertificateList'{tbsCertList = #'TBSCertList'{issuer = Issuer}}}) ->
                        FN0 = base64:encode(crypto:hash(md5, term_to_binary(Issuer))),
                        FN = re:replace(FN0,"/","_",[{return,list}]),
                        file:write_file(filename:join(Dir, vcn_utils:ensure_list(FN) ++ ".crl"), DER)
                    end, CRLs),
                ok
            catch
                Type:Reason1 ->
                    ?error_stacktrace("Failed to save DER CAs and CRLs due to: ~p:~p", [Type, Reason1]),
                    {error, Reason1}
            end
    end.

exec(OneProxyPid, CMD) ->
    OneProxyPid ! {self(), {command, CMD}},
    receive
        Response -> Response
    after 5000 ->
        {error, timeout}
    end.