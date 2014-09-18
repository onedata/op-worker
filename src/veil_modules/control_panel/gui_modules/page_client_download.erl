%% ===================================================================
%% @author Krzysztof Trzepla
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains n2o website code.
%% The page contains information about oneclinet and links for download.
%% @end
%% ===================================================================

-module(page_client_download).
-include("veil_modules/control_panel/common.hrl").
-include("registered_names.hrl").

% n2o API
-export([main/0, event/1]).

% URLs for client packages download
-define(CLIENT_RPM_URL, "http://packages.onedata.org/VeilClient-Linux.rpm").
-define(CLIENT_DEB_URL, "http://packages.onedata.org/VeilClient-Linux.deb").

%% Template points to the template file, which will be filled with content
main() ->
    case vcn_gui_utils:maybe_redirect(true, true, true) of
        true ->
            #dtl{file = "bare", app = veil_cluster_node, bindings = [{title, <<"">>}, {body, <<"">>}, {custom, <<"">>}]};
        false ->
            #dtl{file = "bare", app = veil_cluster_node, bindings = [{title, title()}, {body, body()}, {custom, <<"">>}]}
    end.

%% Page title
title() -> <<"Download oneclient">>.

%% This will be placed in the template instead of {{body}} tag
body() ->
    #panel{style = <<"position: relative;">>, body = [
        vcn_gui_utils:top_menu(data_tab),
        #panel{style = <<"margin-top: 60px; padding: 20px;">>, body = [
            #h6{style = <<" text-align: center;">>, body = <<"Download oneclient">>},
            #panel{id = <<"main_panel">>, body = main_panel()}
        ]}
    ] ++ vcn_gui_utils:logotype_footer(20)}.

main_panel() ->
    [
        #p{style = <<"margin-top: 30px;">>, body = <<"<strong>oneclient</strong> is a software based on FUSE ",
        "(Filesystem in Userspace) that allows mounting onedata filesystem on UNIX systems.">>},
        #p{body = <<"In order to connect to the system, either <strong>access token</strong> ",
        "or <strong>certificate pair</strong> can be used.">>},
        #p{body = <<"Download and install preferred package to mount onedata filesystem on your computer:">>},
        #list{style = <<"margin-top: -3px;">>, numbered = true, body = [
            #li{style = <<"font-size: 18px; padding: 5px 0;">>, body =
            #link{body = <<"RPM package">>, url = <<?CLIENT_RPM_URL>>}},
            #li{style = <<"font-size: 18px; padding: 5px 0;">>, body =
            #link{body = <<"DEB package">>, url = <<?CLIENT_DEB_URL>>}}
        ]}
    ].

event(init) -> ok;
event(terminate) -> ok.
