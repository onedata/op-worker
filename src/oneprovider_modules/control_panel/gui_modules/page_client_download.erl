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

% URLs for client packages download
-define(CLIENT_RPM_URL, <<"http://packages.onedata.org/oneclient-linux.rpm">>).
-define(CLIENT_DEB_URL, <<"http://packages.onedata.org/oneclient-linux.deb">>).

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
    #panel{style = <<"position: relative;">>, body = [
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

        #table{body = [
            #tr{cells = [
                #td{style = <<"padding: 5px;">>, body = [
                    #p{body = <<"Download and install <i>RPM</i> package">>},
                    #pre{body = #code{class = <<"bash">>, body = [
                        <<"curl --url ", (?CLIENT_RPM_URL)/binary, " --output oneclient.rpm<br>sudo yum install oneclient.rpm">>
                    ]}}
                ]},
                #td{style = <<"padding: 5px;">>, body = [
                    #p{body = <<"Download and install <i>DEB</i> package">>},
                    #pre{body = #code{class = <<"bash">>, body = [
                        <<"curl --url ", (?CLIENT_DEB_URL)/binary, " --output oneclient.deb<br>sudo dpkg -i oneclient.deb">>
                    ]}}
                ]}
            ]}
        ]},

        #table{body = [
            #tr{cells = [
                #td{style = <<"width: 50%; padding: 5px;">>, body = [
                    #p{body = <<"Run <strong>oneclient</strong> using certificate">>},
                    #list{body = [
                        #li{style = <<"font-size: 18px; padding: 5px 0;">>, body =
                        <<"Prepare X.509 certificate (acceptable formats are <i>PEM</i> and <i>PKCS 12</i>)">>
                        },
                        #li{style = <<"font-size: 18px; padding: 5px 0;">>, body =
                        <<"Set <i>provider_hostname</i> and <i>peer_certificate_file</i>"
                        " variables in configuration file located at "
                        "<i>{INSTALL_PREFIX}/etc/oneclient.conf.default</i>">>
                        },
                        #li{style = <<"font-size: 18px; padding: 5px 0;">>, body = [
                            <<"Register certificate by pasting its content on ">>,
                            #link{body = <<"manage account page">>, href = <<"/manage_account">>}
                        ]},
                        #li{style = <<"font-size: 18px; padding: 5px 0;">>, body = [
                            <<"Start <strong>oneclient</strong>: ">>,
                            #pre{style = <<"margin: 0 auto; margin-top: 10px;">>, body = #code{
                                class = <<"bash">>, body = <<"oneclient <i>mount-point</i>">>}
                            }
                        ]},
                        #li{style = <<"font-size: 18px; padding: 5px 0;">>, body =
                        <<"Confirm certificate (required only once)">>
                        }
                    ]}
                ]},
                #td{style = <<"width: 50%; padding: 5px;">>, body = [
                    #p{body = <<"Run <strong>oneclient</strong> using token">>},
                    #list{body = [
                        #li{style = <<"font-size: 18px; padding: 5px 0;">>, body =
                        <<"Set <i>provider_hostname</i> variable in configuration file"
                        " located at <i>{INSTALL_PREFIX}/etc/oneclient.conf.default</i>">>
                        },
                        #li{style = <<"font-size: 18px; padding: 5px 0;">>, body = [
                            <<"Generate and copy ">>,
                            #link{body = <<"authorization code">>, href = <<"/tokens">>}
                        ]},
                        #li{style = <<"font-size: 18px; padding: 5px 0;">>, body = [
                            <<"Start <strong>oneclient</strong>: ">>,
                            #pre{style = <<"margin: 0 auto; margin-top: 10px;">>, body = #code{
                                class = <<"bash">>, body = <<"oneclient --authorization token <i>mount-point</i>">>}
                            }
                        ]},
                        #li{style = <<"font-size: 18px; padding: 5px 0;">>, body = <<"Paste copied authorization code">>}
                    ]}
                ]}
            ]}
        ]},

        #h6{style = <<"text-align: center;">>, body = <<"Congratulations! You have successfully mounted <strong>onedata</strong> filesystem.">>}

    ].

event(init) -> ok;
event(terminate) -> ok.
