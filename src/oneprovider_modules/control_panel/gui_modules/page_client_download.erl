%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains n2o website code.
%% The page contains information about oneclinet and links for download.
%% @end
%% ===================================================================

-module(page_client_download).
-include("oneprovider_modules/control_panel/common.hrl").
-include("registered_names.hrl").

% n2o API
-export([main/0, event/1]).

%% Template points to the template file, which will be filled with content
main() ->
    case opn_gui_utils:maybe_redirect(true, false) of
        true ->
            #dtl{file = "bare", app = ?APP_Name, bindings = [{title, <<"">>}, {body, <<"">>}, {custom, <<"">>}]};
        false ->
            #dtl{file = "bare", app = ?APP_Name, bindings = [{title, title()}, {body, body()}, {custom, custom()}, {css, css()}]}
    end.

%% Page title
title() -> <<"Download oneclient">>.

custom() ->
    <<"<script src=\"/flatui/highlight.min.js\" type=\"text/javascript\" charset=\"utf-8\"></script>\n",
    "<script>hljs.initHighlightingOnLoad();</script>">>.

css() ->
    <<"<link rel=\"stylesheet\" href=\"/flatui/highlight.min.css\" type=\"text/css\" media=\"screen\" charset=\"utf-8\" />">>.

%% This will be placed in the template instead of {{body}} tag
body() ->
    #panel{class = <<"page-container">>, body = [
        opn_gui_utils:top_menu(client_download_tab),
        #panel{style = <<"margin-top: 60px; padding: 20px;">>, body = [
            #h6{style = <<"text-align: center;">>, body = <<"Oneclient">>},
            #panel{style = <<"width: 90%; margin: 0 auto;">>, body = main_panel()}
        ]}
    ]}.

main_panel() ->
    [
        #p{style = <<"text-align: center;">>, body = <<"<strong>oneclient</strong> is a software based on FUSE ",
        "(Filesystem in Userspace) that allows mounting <strong>onedata</strong> filesystem on Linux systems.">>},

        #p{style = <<"font-size: 20px; margin-top: 30px;">>, body = <<"Download and install the <i>RPM</i> package">>},

        #pre{body = #code{class = <<"bash">>, body = [
            <<"rpm --import http://packages.onedata.org/GPG-KEY-onedata <br>"
            "sudo wget -qO /etc/yum.repos.d/onedata.repo http://packages.onedata.org/onedata.repo <br>"
            "yum install oneprovider">>
        ]}},

        #p{style = <<"font-size: 20px; margin-top: 30px;">>, body = <<"Download and install the <i>DEB</i> package">>},

        #pre{body = #code{class = <<"bash">>, body = [
            <<"wget -qO - http://packages.onedata.org/GPG-KEY-onedata | sudo apt-key add - <br>"
            "echo 'deb http://packages.onedata.org/debian/ testing main' >> /etc/apt/sources.list <br>"
            "sudo apt-get update && sudo apt-get install oneprovider">>
        ]}},

        #p{style = <<"font-size: 20px; margin-top: 30px;">>, body = <<"Run <strong>oneclient</strong> using a <i>certificate</i>">>},

        #list{body = [
            #li{style = <<"font-size: 18px; padding: 5px 0;">>, body =
            <<"Place your X.509 certificate (acceptable formats are <i>PEM</i> and <i>PKCS 12</i>)",
            " in <i>$HOME/.globus/usercert.pem</i>">>
            },
            #li{style = <<"font-size: 18px; padding: 5px 0;">>, body = [
                <<"Register the certificate by pasting its content on ">>,
                #link{body = <<"manage account page">>, href = <<"/manage_account">>}
            ]},
            #li{style = <<"font-size: 18px; padding: 5px 0;">>, body = [
                <<"Start <strong>oneclient</strong>: ">>,
                #pre{style = <<"margin: 0 auto; margin-top: 10px;">>, body = #code{
                    class = <<"bash">>, body = <<"oneclient --no-check-certificate <i>mount-point</i>">>}
                }
            ]},
            #li{style = <<"font-size: 18px; padding: 5px 0;">>, body =
            <<"Confirm the certificate (required only once)">>
            }
        ]},

        #p{style = <<"font-size: 20px; margin-top: 30px;">>, body = <<"Run <strong>oneclient</strong> using a <i>token</i>">>},

        #list{body = [
            #li{style = <<"font-size: 18px; padding: 5px 0;">>, body = [
                <<"Generate and copy an ">>,
                #link{body = <<"authorization code">>, href = <<"/tokens">>}
            ]},
            #li{style = <<"font-size: 18px; padding: 5px 0;">>, body = [
                <<"Start <strong>oneclient</strong>: ">>,
                #pre{style = <<"margin: 0 auto; margin-top: 10px;">>, body = #code{
                    class = <<"bash">>, body = <<"oneclient --authentication token <i>mount-point</i>">>}
                }
            ]},
            #li{style = <<"font-size: 18px; padding: 5px 0;">>, body = <<"Paste the authorization code">>}
        ]},

        #h6{style = <<"text-align: center; margin-top: 30px;">>, body = <<"Congratulations! You have successfully mounted <strong>onedata</strong> filesystem.">>}

    ].

event(init) -> ok;
event(terminate) -> ok.

