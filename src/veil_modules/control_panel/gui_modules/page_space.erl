% ===================================================================
%% @author Krzysztof Trzepla
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains n2o website code.
%% The page allows user to manage his Space.
%% @end
%% ===================================================================

-module(page_space).
-include("veil_modules/control_panel/common.hrl").
-include("veil_modules/dao/dao_spaces.hrl").
-include_lib("ctool/include/logging.hrl").

% n2o API and comet
-export([main/0, event/1, comet_loop/1]).

%% Comet process pid
-define(COMET_PID, comet_pid).

%% Comet process state
-define(STATE, state).
-record(?STATE, {spaceId}).

%% Common page CCS styles
-define(MESSAGE_STYLE, <<"position: fixed; width: 100%; top: 55px; z-index: 1; display: none;">>).
-define(CONTENT_COLUMN_STYLE, <<"padding-right: 0">>).
-define(NAVIGATION_COLUMN_STYLE, <<"border-left-width: 0; width: 20px; padding-left: 0;">>).
-define(DESCRIPTION_STYLE, <<"border-width: 0; text-align: right; width: 10%; padding-left: 0; padding-right: 0;">>).
-define(MAIN_STYLE, <<"border-width: 0;  text-align: left; padding-left: 1em; width: 90%;">>).
-define(LABEL_STYLE, <<"margin: 0 auto;">>).
-define(PARAGRAPH_STYLE, <<"margin: 0 auto;">>).
-define(TABLE_STYLE, <<"border-width: 0; width: 100%; border-collapse: inherit;">>).

%% ====================================================================
%% API functions
%% ====================================================================


%% main/0
%% ====================================================================
%% @doc Template points to the template file, which will be filled with content.
-spec main() -> #dtl{}.
%% ====================================================================
main() ->
    case vcn_gui_utils:maybe_redirect(true, false, false, true) of
        true ->
            #dtl{file = "bare", app = veil_cluster_node, bindings = [{title, <<"">>}, {body, <<"">>}, {custom, <<"">>}]};
        false ->
            #dtl{file = "bare", app = veil_cluster_node, bindings = [{title, title()}, {body, body()}, {custom, custom()}]}
    end.


%% title/0
%% ====================================================================
%% @doc Page title.
-spec title() -> binary().
%% ====================================================================
title() -> <<"Manage Space">>.


%% custom/0
%% ====================================================================
%% @doc This will be placed instead of {{custom}} tag in template.
-spec custom() -> binary().
%% ====================================================================
custom() ->
    <<"<script src='/js/bootbox.min.js' type='text/javascript' charset='utf-8'></script>">>.


%% body/0
%% ====================================================================
%% @doc This will be placed instead of {{body}} tag in template.
-spec body() -> [#panel{}].
%% ====================================================================
body() ->
    case gui_ctx:url_param(<<"id">>) of
        undefined ->
            page_error:redirect_with_error(?error_space_not_found),
            [];
        Id ->
            SpaceId = gui_str:to_binary(Id),
            case gr_adapter:get_space_info(SpaceId, gui_ctx:get_access_token()) of
                {ok, SpaceInfo} -> space(SpaceInfo);
                _ ->
                    page_error:redirect_with_error(?error_space_permission_denied),
                    []
            end
    end.


%% space/1
%% ====================================================================
%% @doc Renders Space settings.
-spec space(SpaceInfo :: #space_info{}) -> [#panel{}].
%% ====================================================================
space(#space_info{space_id = SpaceId, name = Name}) ->
    [
        #panel{
            id = <<"main_spinner">>,
            style = <<"position: absolute; top: 12px; left: 17px; z-index: 1234; width: 32px; display: none;">>,
            body = #image{
                image = <<"/images/spinner.gif">>
            }
        },
        vcn_gui_utils:top_menu(spaces_tab),
        #panel{
            id = <<"ok_message">>,
            style = ?MESSAGE_STYLE,
            class = <<"dialog dialog-success">>
        },
        #panel{
            id = <<"error_message">>,
            style = ?MESSAGE_STYLE,
            class = <<"dialog dialog-danger">>
        },
        #panel{
            style = <<"margin-bottom: 100px;">>,
            body = [
                #h6{
                    style = <<"font-size: x-large; margin: 0 auto; margin-top: 160px; width: 152px;">>,
                    body = <<"Manage Space">>
                },
                #table{
                    style = <<"margin: 0 auto; width: 50%; margin-top: 30px; border-spacing: 1em;
                    border-width: 0; border-collapse: inherit;">>,
                    body = lists:map(fun({Description, MainId, Main}) ->
                        #tr{
                            cells = [
                                #td{
                                    style = ?DESCRIPTION_STYLE,
                                    body = #label{
                                        style = ?LABEL_STYLE,
                                        class = <<"label label-large label-inverse">>,
                                        body = Description
                                    }
                                },
                                #td{
                                    id = MainId,
                                    style = ?MAIN_STYLE,
                                    body = Main
                                }
                            ]
                        }
                    end, [
                        {<<"Space Name">>, <<"space_name">>, space_name(SpaceId, Name)},
                        {<<"Space ID">>, <<"">>, #p{style = ?PARAGRAPH_STYLE, body = SpaceId}}
                    ])
                },
                #table{
                    class = <<"table table-bordered table-striped">>,
                    style = <<"width: 50%; margin: 0 auto; margin-top: 30px; table-layout: fixed;">>,
                    body = #tbody{
                        id = <<"providers">>,
                        body = #tr{
                            cells = [
                                #th{
                                    style = <<"font-size: large;">>,
                                    body = <<"Providers">>
                                },
                                #th{
                                    style = ?NAVIGATION_COLUMN_STYLE,
                                    body = spinner()
                                }
                            ]
                        }
                    }
                },
                #panel{
                    style = <<"margin: 0 auto; width: 50%; margin-top: 30px; text-align: center;">>,
                    body = #button{
                        id = <<"request_support">>,
                        postback = {request_support, SpaceId},
                        class = <<"btn btn-primary">>,
                        body = <<"Request support">>
                    }
                },
                #table{
                    class = <<"table table-bordered table-striped">>,
                    style = <<"width: 50%; margin: 0 auto; margin-top: 30px; table-layout: fixed;">>,
                    body = #tbody{
                        id = <<"users">>,
                        body = #tr{
                            cells = [
                                #th{
                                    style = <<"font-size: large;">>,
                                    body = <<"Users">>
                                },
                                #th{
                                    style = ?NAVIGATION_COLUMN_STYLE,
                                    body = spinner()
                                }
                            ]
                        }
                    }
                },
                #panel{
                    style = <<"margin: 0 auto; width: 50%; margin-top: 30px; text-align: center;">>,
                    body = #button{
                        id = <<"invite_user">>,
                        postback = {invite_user, SpaceId},
                        class = <<"btn btn-primary">>,
                        body = <<"Invite user">>
                    }
                },
                #table{
                    class = <<"table table-bordered table-striped">>,
                    style = <<"width: 50%; margin: 0 auto; margin-top: 30px; table-layout: fixed;">>,
                    body = #tbody{
                        id = <<"groups">>,
                        body = #tr{
                            cells = [
                                #th{
                                    style = <<"font-size: large;">>,
                                    body = <<"Groups">>
                                },
                                #th{
                                    style = ?NAVIGATION_COLUMN_STYLE,
                                    body = spinner()
                                }
                            ]
                        }
                    }
                }
            ]
        }
    ].


%% space_name/2
%% ====================================================================
%% @doc Renders editable Space name.
-spec space_name(SpaceId :: binary(), Name :: binary()) -> Result when
    Result :: #span{}.
%% ====================================================================
space_name(SpaceId, Name) ->
    #span{
        style = <<"font-size: large;">>,
        body = [
            Name,
            #link{
                title = <<"Edit">>,
                style = <<"margin-left: 1em;">>,
                class = <<"glyph-link">>,
                postback = {change_space_name, SpaceId, Name},
                body = #span{
                    class = <<"fui-new">>
                }
            }
        ]
    }.


%% change_space_name/2
%% ====================================================================
%% @doc Renders change space_name input field.
-spec change_space_name(SpaceId :: binary(), Name :: binary()) -> Result when
    Result :: list().
%% ====================================================================
change_space_name(SpaceId, Name) ->
    [
        #textbox{
            id = <<"new_space_name_textbox">>,
            class = <<"span">>,
            placeholder = <<"New Space name">>
        },
        #link{
            id = <<"new_space_name_submit">>,
            class = <<"glyph-link">>,
            style = <<"margin-left: 1em;">>,
            title = <<"Submit">>,
            actions = gui_jq:form_submit_action(<<"new_space_name_submit">>, {submit_new_space_name, SpaceId, Name}, <<"new_space_name_textbox">>),
            body = #span{
                class = <<"fui-check-inverted">>,
                style = <<"font-size: large;">>
            }
        },
        #link{
            class = <<"glyph-link">>,
            style = <<"margin-left: 10px;">>,
            title = <<"Cancel">>,
            postback = {cancel_new_space_name_submit, SpaceId, Name},
            body = #span{
                class = <<"fui-cross-inverted">>,
                style = <<"font-size: large;">>
            }
        }
    ].


%% providers_table_collapsed/2
%% ====================================================================
%% @doc Renders collapsed providers table for given Space.
-spec providers_table_collapsed(SpaceId :: binary()) -> Result when
    Result :: [#tr{}].
%% ====================================================================
providers_table_collapsed(SpaceId) ->
    SpinnerId = <<"providers_spinner">>,
    Header = #tr{
        cells = [
            #th{
                style = <<"font-size: large;">>,
                body = <<"Providers">>
            },
            #th{
                id = SpinnerId,
                style = ?NAVIGATION_COLUMN_STYLE,
                body = expand_button(<<"Expand All">>, {providers_table_expand, SpaceId, SpinnerId})
            }
        ]
    },
    try
        {ok, ProviderIds} = gr_adapter:get_space_providers(SpaceId, gui_ctx:get_access_token()),
        Rows = lists:map(fun({ProviderId, Counter}) ->
            RowId = <<"provider_", (integer_to_binary(Counter))/binary>>,
            #tr{
                id = RowId,
                cells = provider_row_collapsed(SpaceId, ProviderId, RowId)
            }
        end, lists:zip(ProviderIds, tl(lists:seq(0, length(ProviderIds))))),
        [Header | Rows]
    catch
        _:_ ->
            message(<<"error_message">>, <<"Cannot fetch Space providers.<br>Please try again later.">>),
            [Header]
    end.


%% providers_table_expanded/1
%% ====================================================================
%% @doc Renders expanded providers table for given Space.
-spec providers_table_expanded(SpaceId :: binary()) -> Result when
    Result :: [#tr{}].
%% ====================================================================
providers_table_expanded(SpaceId) ->
    SpinnerId = <<"providers_spinner">>,
    Header = #tr{
        cells = [
            #th{
                style = <<"font-size: large;">>,
                body = <<"Providers">>
            },
            #th{
                id = SpinnerId,
                style = ?NAVIGATION_COLUMN_STYLE,
                body = collapse_button(<<"Collapse All">>, {providers_table_collapse, SpaceId, SpinnerId})
            }
        ]
    },
    try
        {ok, ProviderIds} = gr_adapter:get_space_providers(SpaceId, gui_ctx:get_access_token()),
        Rows = lists:map(fun({ProviderId, Counter}) ->
            RowId = <<"provider_", (integer_to_binary(Counter))/binary>>,
            #tr{
                id = RowId,
                cells = provider_row_expanded(SpaceId, ProviderId, RowId)
            }
        end, lists:zip(ProviderIds, tl(lists:seq(0, length(ProviderIds))))),
        [Header | Rows]
    catch
        _:_ ->
            message(<<"error_message">>, <<"Cannot fetch Space providers.<br>Please try again later.">>),
            [Header]
    end.


%% provider_row_collapsed/3
%% ====================================================================
%% @doc Renders collapsed provider row for given Space.
-spec provider_row_collapsed(SpaceId :: binary(), ProviderId :: binary(), RowId :: binary()) -> Result when
    Result :: [#td{}].
%% ====================================================================
provider_row_collapsed(SpaceId, ProviderId, RowId) ->
    SpinnerId = <<RowId/binary, "_spinner">>,
    [
        #td{
            style = ?CONTENT_COLUMN_STYLE,
            body = #table{
                style = ?TABLE_STYLE,
                body = [
                    #tr{
                        cells = [
                            #td{
                                style = ?DESCRIPTION_STYLE,
                                body = #label{
                                    style = ?LABEL_STYLE,
                                    class = <<"label label-large label-inverse">>,
                                    body = <<"Provider ID">>
                                }
                            },
                            #td{
                                style = ?MAIN_STYLE,
                                body = #p{
                                    style = ?PARAGRAPH_STYLE,
                                    body = ProviderId
                                }
                            }
                        ]
                    }
                ]
            }
        },
        #td{
            id = SpinnerId,
            style = ?NAVIGATION_COLUMN_STYLE,
            body = expand_button({provider_row_expand, SpaceId, ProviderId, RowId, SpinnerId})
        }
    ].


%% provider_row_expanded/3
%% ====================================================================
%% @doc Renders expanded provider row for given Space.
-spec provider_row_expanded(SpaceId :: binary(), ProviderId :: binary(), RowId :: binary()) -> Result when
    Result :: [#td{}].
%% ====================================================================
provider_row_expanded(SpaceId, ProviderId, RowId) ->
    try
        {ok, {_, Urls, RedirectionPoint}} =
            gr_adapter:get_provider_details(SpaceId, ProviderId, gui_ctx:get_access_token()),
        SpinnerId = <<RowId/binary, "_spinner">>,
        [
            #td{
                style = ?CONTENT_COLUMN_STYLE,
                body = [
                    #table{
                        style = ?TABLE_STYLE,
                        body = [
                            #tr{
                                cells = [
                                    #td{
                                        style = ?DESCRIPTION_STYLE,
                                        body = #label{
                                            style = ?LABEL_STYLE,
                                            class = <<"label label-large label-inverse">>,
                                            body = <<"Provider ID">>
                                        }
                                    },
                                    #td{
                                        style = ?MAIN_STYLE,
                                        body = #p{
                                            style = ?PARAGRAPH_STYLE,
                                            body = ProviderId
                                        }
                                    }
                                ]
                            },
                            #tr{
                                cells = [
                                    #td{
                                        style = <<(?DESCRIPTION_STYLE)/binary, " vertical-align: top;">>,
                                        body = #label{
                                            style = ?LABEL_STYLE,
                                            class = <<"label label-large label-inverse">>,
                                            body = <<"URLs">>
                                        }
                                    },
                                    #td{
                                        style = ?MAIN_STYLE,
                                        body = #list{
                                            style = <<"list-style-type: none; margin: 0 auto;">>,
                                            body = lists:map(fun(Url) ->
                                                #li{body = #p{
                                                    style = ?PARAGRAPH_STYLE,
                                                    body = Url}
                                                }
                                            end, Urls)
                                        }
                                    }
                                ]
                            },
                            #tr{
                                cells = [
                                    #td{
                                        style = ?DESCRIPTION_STYLE,
                                        body = #label{
                                            style = ?LABEL_STYLE,
                                            class = <<"label label-large label-inverse">>,
                                            body = <<"Redirection point">>
                                        }
                                    },
                                    #td{
                                        style = ?MAIN_STYLE,
                                        body = #p{
                                            style = ?PARAGRAPH_STYLE,
                                            body = RedirectionPoint
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            },
            #td{
                id = SpinnerId,
                style = ?NAVIGATION_COLUMN_STYLE,
                body = collapse_button({provider_row_collapse, SpaceId, ProviderId, RowId, SpinnerId})
            }
        ]
    catch
        _:_ ->
            message(<<"error_message">>, <<"Cannot fetch Space provider details.<br>Please try again later.">>),
            provider_row_collapsed(SpaceId, ProviderId, RowId)
    end.


%% users_table_collapsed/2
%% ====================================================================
%% @doc Renders collapsed users table for given Space.
-spec users_table_collapsed(SpaceId :: binary()) -> Result when
    Result :: [#tr{}].
%% ====================================================================
users_table_collapsed(SpaceId) ->
    SpinnerId = <<"users_spinner">>,
    Header = #tr{
        cells = [
            #th{
                style = <<"font-size: large;">>,
                body = <<"Users">>
            },
            #th{
                id = SpinnerId,
                style = ?NAVIGATION_COLUMN_STYLE,
                body = expand_button(<<"Expand All">>, {users_table_expand, SpaceId, SpinnerId})
            }
        ]
    },
    try
        {ok, UserIds} = gr_adapter:get_space_users(SpaceId, gui_ctx:get_access_token()),
        Rows = lists:map(fun({UserId, Counter}) ->
            RowId = <<"user_", (integer_to_binary(Counter))/binary>>,
            #tr{
                id = RowId,
                cells = user_row_collapsed(SpaceId, UserId, RowId)
            }
        end, lists:zip(UserIds, tl(lists:seq(0, length(UserIds))))),
        [Header | Rows]
    catch
        _:_ ->
            message(<<"error_message">>, <<"Cannot fetch Space users.<br>Please try again later.">>),
            [Header]
    end.


%% users_table_expanded/1
%% ====================================================================
%% @doc Renders expanded users table for given Space.
-spec users_table_expanded(SpaceId :: binary()) -> Result when
    Result :: [#tr{}].
%% ====================================================================
users_table_expanded(SpaceId) ->
    SpinnerId = <<"users_spinner">>,
    Header = #tr{
        cells = [
            #th{
                style = <<"font-size: large;">>,
                body = <<"Users">>
            },
            #th{
                id = SpinnerId,
                style = ?NAVIGATION_COLUMN_STYLE,
                body = collapse_button(<<"Collapse All">>, {users_table_collapse, SpaceId, SpinnerId})
            }
        ]
    },
    try
        {ok, UserIds} = gr_adapter:get_space_users(SpaceId, gui_ctx:get_access_token()),
        Rows = lists:map(fun({UserId, Counter}) ->
            RowId = <<"user_", (integer_to_binary(Counter))/binary>>,
            #tr{
                id = RowId,
                cells = user_row_expanded(SpaceId, UserId, RowId)
            }
        end, lists:zip(UserIds, tl(lists:seq(0, length(UserIds))))),
        [Header | Rows]
    catch
        _:_ ->
            message(<<"error_message">>, <<"Cannot fetch Space users.<br>Please try again later.">>),
            [Header]
    end.


%% user_row_collapsed/3
%% ====================================================================
%% @doc Renders collapsed user row for given Space.
-spec user_row_collapsed(SpaceId :: binary(), UserId :: binary(), RowId :: binary()) -> Result when
    Result :: [#td{}].
%% ====================================================================
user_row_collapsed(SpaceId, UserId, RowId) ->
    try
        {ok, {_, Name}} = gr_adapter:get_user_details(SpaceId, UserId, gui_ctx:get_access_token()),
        SpinnerId = <<RowId/binary, "_spinner">>,
        [
            #td{
                style = ?CONTENT_COLUMN_STYLE,
                body = #table{
                    style = ?TABLE_STYLE,
                    body = lists:map(fun({Description, Main}) ->
                        #tr{
                            cells = [
                                #td{
                                    style = ?DESCRIPTION_STYLE,
                                    body = #label{
                                        style = ?LABEL_STYLE,
                                        class = <<"label label-large label-inverse">>,
                                        body = Description
                                    }
                                },
                                #td{
                                    style = ?MAIN_STYLE,
                                    body = #p{
                                        style = ?PARAGRAPH_STYLE,
                                        body = Main
                                    }
                                }
                            ]
                        }
                    end, [{<<"Name">>, Name}])
                }
            },
            #td{
                id = SpinnerId,
                style = ?NAVIGATION_COLUMN_STYLE,
                body = expand_button({user_row_expand, SpaceId, UserId, RowId, SpinnerId})
            }
        ]
    catch
        _:_ ->
            message(<<"error_message">>, <<"Cannot fetch Space user details.<br>Please try again later.">>),
            []
    end.


%% user_row_expanded/3
%% ====================================================================
%% @doc Renders expanded user row for given Space.
-spec user_row_expanded(SpaceId :: binary(), UserId :: binary(), RowId :: binary()) -> Result when
    Result :: [#td{}].
%% ====================================================================
user_row_expanded(SpaceId, UserId, RowId) ->
    try
        {ok, {_, Name}} = gr_adapter:get_user_details(SpaceId, UserId, gui_ctx:get_access_token()),
        SpinnerId = <<RowId/binary, "_spinner">>,
        [
            #td{
                style = ?CONTENT_COLUMN_STYLE,
                body = #table{
                    style = ?TABLE_STYLE,
                    body = lists:map(fun({Description, Main}) ->
                        #tr{
                            cells = [
                                #td{
                                    style = ?DESCRIPTION_STYLE,
                                    body = #label{
                                        style = ?LABEL_STYLE,
                                        class = <<"label label-large label-inverse">>,
                                        body = Description
                                    }
                                },
                                #td{
                                    style = ?MAIN_STYLE,
                                    body = #p{
                                        style = ?PARAGRAPH_STYLE,
                                        body = Main
                                    }
                                }
                            ]
                        }
                    end, [{<<"Name">>, Name}, {<<"User ID">>, UserId}])
                }
            },
            #td{
                id = SpinnerId,
                style = ?NAVIGATION_COLUMN_STYLE,
                body = collapse_button({user_row_collapse, SpaceId, UserId, RowId, SpinnerId})
            }
        ]
    catch
        _:_ ->
            message(<<"error_message">>, <<"Cannot fetch Space user details.<br>Please try again later.">>),
            []
    end.


%% groups_table_collapsed/2
%% ====================================================================
%% @doc Renders collapsed groups table for given Space.
-spec groups_table_collapsed(SpaceId :: binary()) -> Result when
    Result :: [#tr{}].
%% ====================================================================
groups_table_collapsed(SpaceId) ->
    SpinnerId = <<"groups_spinner">>,
    Header = #tr{
        cells = [
            #th{
                style = <<"font-size: large;">>,
                body = <<"Groups">>
            },
            #th{
                id = SpinnerId,
                style = ?NAVIGATION_COLUMN_STYLE,
                body = expand_button(<<"Expand All">>, {groups_table_expand, SpaceId, SpinnerId})
            }
        ]
    },
    try
        {ok, GroupIds} = gr_adapter:get_space_groups(SpaceId, gui_ctx:get_access_token()),
        Rows = lists:map(fun({GroupId, Counter}) ->
            RowId = <<"group_", (integer_to_binary(Counter))/binary>>,
            #tr{
                id = RowId,
                cells = group_row_collapsed(SpaceId, GroupId, RowId)
            }
        end, lists:zip(GroupIds, tl(lists:seq(0, length(GroupIds))))),
        [Header | Rows]
    catch
        _:_ ->
            message(<<"error_message">>, <<"Cannot fetch Space groups.<br>Please try again later.">>),
            [Header]
    end.


%% groups_table_expanded/1
%% ====================================================================
%% @doc Renders expanded groups table for given Space.
-spec groups_table_expanded(SpaceId :: binary()) -> Result when
    Result :: [#tr{}].
%% ====================================================================
groups_table_expanded(SpaceId) ->
    SpinnerId = <<"groups_spinner">>,
    Header = #tr{
        cells = [
            #th{
                style = <<"font-size: large;">>,
                body = <<"Groups">>
            },
            #th{
                id = SpinnerId,
                style = ?NAVIGATION_COLUMN_STYLE,
                body = collapse_button(<<"Collapse All">>, {groups_table_collapse, SpaceId, SpinnerId})
            }
        ]
    },
    try
        {ok, GroupIds} = gr_adapter:get_space_groups(SpaceId, gui_ctx:get_access_token()),
        Rows = lists:map(fun({GroupId, Counter}) ->
            RowId = <<"group_", (integer_to_binary(Counter))/binary>>,
            #tr{
                id = RowId,
                cells = group_row_expanded(SpaceId, GroupId, RowId)
            }
        end, lists:zip(GroupIds, tl(lists:seq(0, length(GroupIds))))),
        [Header | Rows]
    catch
        _:_ ->
            message(<<"error_message">>, <<"Cannot fetch Space groups.<br>Please try again later.">>),
            [Header]
    end.


%% group_row_collapsed/3
%% ====================================================================
%% @doc Renders collapsed group row for given Space.
-spec group_row_collapsed(SpaceId :: binary(), GroupId :: binary(), RowId :: binary()) -> Result when
    Result :: [#td{}].
%% ====================================================================
group_row_collapsed(SpaceId, GroupId, RowId) ->
    try
        {ok, {_, Name}} = gr_adapter:get_group_details(SpaceId, GroupId, gui_ctx:get_access_token()),
        SpinnerId = <<RowId/binary, "_spinner">>,
        [
            #td{
                style = ?CONTENT_COLUMN_STYLE,
                body = #table{
                    style = ?TABLE_STYLE,
                    body = [
                        #tr{
                            cells = [
                                #td{
                                    style = ?DESCRIPTION_STYLE,
                                    body = #label{
                                        style = ?LABEL_STYLE,
                                        class = <<"label label-large label-inverse">>,
                                        body = <<"Name">>
                                    }
                                },
                                #td{
                                    style = ?MAIN_STYLE,
                                    body = #p{
                                        style = ?PARAGRAPH_STYLE,
                                        body = Name
                                    }
                                }
                            ]
                        }
                    ]
                }
            },
            #td{
                id = SpinnerId,
                style = ?NAVIGATION_COLUMN_STYLE,
                body = expand_button({group_row_expand, SpaceId, GroupId, RowId, SpinnerId})
            }
        ]
    catch
        _:_ ->
            message(<<"error_message">>, <<"Cannot fetch Space group details.<br>Please try again later.">>),
            []
    end.


%% group_row_expanded/3
%% ====================================================================
%% @doc Renders expanded group row for given Space.
-spec group_row_expanded(SpaceId :: binary(), GroupId :: binary(), RowId :: binary()) -> Result when
    Result :: [#td{}].
%% ====================================================================
group_row_expanded(SpaceId, GroupId, RowId) ->
    try
        {ok, {_, Name}} = gr_adapter:get_group_details(SpaceId, GroupId, gui_ctx:get_access_token()),
        SpinnerId = <<RowId/binary, "_spinner">>,
        [
            #td{
                style = ?CONTENT_COLUMN_STYLE,
                body = #table{
                    style = ?TABLE_STYLE,
                    body = lists:map(fun({Description, Main}) ->
                        #tr{
                            cells = [
                                #td{
                                    style = ?DESCRIPTION_STYLE,
                                    body = #label{
                                        style = ?LABEL_STYLE,
                                        class = <<"label label-large label-inverse">>,
                                        body = Description
                                    }
                                },
                                #td{
                                    style = ?MAIN_STYLE,
                                    body = #p{
                                        style = ?PARAGRAPH_STYLE,
                                        body = Main
                                    }
                                }
                            ]
                        }
                    end, [{<<"Name">>, Name}, {<<"Group ID">>, GroupId}])
                }
            },
            #td{
                id = SpinnerId,
                style = ?NAVIGATION_COLUMN_STYLE,
                body = collapse_button({group_row_collapse, SpaceId, GroupId, RowId, SpinnerId})
            }
        ]
    catch
        _:_ ->
            message(<<"error_message">>, <<"Cannot fetch Space group details.<br>Please try again later.">>),
            []
    end.


%% collapse_button/1
%% ====================================================================
%% @doc Renders collapse button.
-spec collapse_button(Postback :: term()) -> Result when
    Result :: #link{}.
%% ====================================================================
collapse_button(Postback) ->
    collapse_button(<<"Collapse">>, Postback).


%% collapse_button/2
%% ====================================================================
%% @doc Renders collapse button.
-spec collapse_button(Title :: binary(), Postback :: term()) -> Result when
    Result :: #link{}.
%% ====================================================================
collapse_button(Title, Postback) ->
    #link{
        title = Title,
        class = <<"glyph-link">>,
        postback = Postback,
        body = #span{
            style = <<"font-size: large; vertical-align: top;">>,
            class = <<"fui-triangle-up">>
        }
    }.


%% expand_button/1
%% ====================================================================
%% @doc Renders expand button.
-spec expand_button(Postback :: term()) -> Result when
    Result :: #link{}.
%% ====================================================================
expand_button(Postback) ->
    expand_button(<<"Expand">>, Postback).


%% expand_button/2
%% ====================================================================
%% @doc Renders expand button.
-spec expand_button(Title :: binary(), Postback :: term()) -> Result when
    Result :: #link{}.
%% ====================================================================
expand_button(Title, Postback) ->
    #link{
        title = Title,
        class = <<"glyph-link">>,
        postback = Postback,
        body = #span{
            style = <<"font-size: large;  vertical-align: top;">>,
            class = <<"fui-triangle-down">>
        }
    }.


%% message/3
%% ====================================================================
%% @doc Renders a message in given element and allows to hide this message.
-spec message(Id :: binary(), Message :: binary()) -> Result when
    Result :: ok.
%% ====================================================================
message(Id, Message) ->
    Body = [
        Message,
        #link{
            title = <<"Close">>,
            style = <<"position: absolute; right: 1em; top: 1em;">>,
            class = <<"glyph-link">>,
            postback = {close_message, Id},
            body = #span{
                class = <<"fui-cross">>
            }
        }
    ],
    gui_jq:update(Id, Body),
    gui_jq:fade_in(Id, 300).


%% spinner/0
%% ====================================================================
%% @doc Renders spinner GIF.
-spec spinner() -> Result when
    Result :: #image{}.
%% ====================================================================
spinner() ->
    #image{
        image = <<"/images/spinner.gif">>,
        style = <<"width: 1.5em;">>
    }.


%% alert_popup/3
%% ====================================================================
%% @doc Displays custom alert popup.
-spec alert_popup(Title :: binary(), Message :: binary(), Script :: binary()) -> binary().
%% ====================================================================
alert_popup(Title, Message, Script) ->
    gui_jq:wire(<<"var box = bootbox.dialog({
        title: '", Title/binary, "',
        message: '", Message/binary, "',
        buttons: {
            'OK': {
                className: 'btn-primary confirm',
                callback: function() {", Script/binary, "}
            }
        }
    });">>).


%% bind_key_to_click/2
%% ====================================================================
%% @doc Makes any keypresses of given key to click on selected class.
%% @end
-spec bind_key_to_click(KeyCode :: binary(), TargetID :: binary()) -> string().
%% ====================================================================
bind_key_to_click(KeyCode, TargetID) ->
    Script = <<"$(document).bind('keydown', function (e){",
    "if (e.which == ", KeyCode/binary, ") { e.preventDefault(); $('", TargetID/binary, "').click(); } });">>,
    gui_jq:wire(Script, false).


%% comet_loop/1
%% ====================================================================
%% @doc Handles space management actions.
-spec comet_loop(State :: #?STATE{}) -> Result when
    Result :: {error, Reason :: term()}.
%% ====================================================================
comet_loop({error, Reason}) ->
    {error, Reason};

comet_loop(#?STATE{} = State) ->
    NewState = try
        receive
            {request_support, SpaceId} ->
                case gr_adapter:request_support(SpaceId, gui_ctx:get_access_token()) of
                    {ok, Token} ->
                        Message = <<"Give underlying token to any Provider that is willing to support your Space.",
                        "<input type=\"text\" style=\"margin-top: 1em; width: 80%;\" value=\"", Token/binary, "\">">>,
                        alert_popup(<<"Request support">>, Message, <<"return true;">>),
                        gui_comet:flush();
                    _ ->
                        message(<<"error_message">>, <<"Cannot get Space support token.<br>Please try again later.">>)
                end,
                gui_jq:hide(<<"main_spinner">>),
                gui_comet:flush(),
                State;

            {invite_user, SpaceId} ->
                case gr_adapter:invite_user(SpaceId, gui_ctx:get_access_token()) of
                    {ok, Token} ->
                        Message = <<"Give underlying token to any User that is willing to join your Space.",
                        "<input type=\"text\" style=\"margin-top: 1em; width: 80%;\" value=\"", Token/binary, "\">">>,
                        alert_popup(<<"Invite user">>, Message, <<"return true;">>),
                        gui_comet:flush();
                    _ ->
                        message(<<"error_message">>, <<"Cannot get Space invitation token.<br>Please try again later.">>)
                end,
                gui_jq:hide(<<"main_spinner">>),
                gui_comet:flush(),
                State;

            {providers_table_collapse, SpaceId} ->
                gui_jq:update(<<"providers">>, providers_table_collapsed(SpaceId)),
                gui_comet:flush(),
                State;

            {providers_table_expand, SpaceId} ->
                gui_jq:update(<<"providers">>, providers_table_expanded(SpaceId)),
                gui_comet:flush(),
                State;

            {provider_row_collapse, SpaceId, ProviderId, RowId} ->
                gui_jq:update(RowId, provider_row_collapsed(SpaceId, ProviderId, RowId)),
                gui_comet:flush(),
                State;

            {provider_row_expand, SpaceId, ProviderId, RowId} ->
                gui_jq:update(RowId, provider_row_expanded(SpaceId, ProviderId, RowId)),
                gui_comet:flush(),
                State;

            {users_table_collapse, SpaceId} ->
                gui_jq:update(<<"users">>, users_table_collapsed(SpaceId)),
                gui_comet:flush(),
                State;

            {users_table_expand, SpaceId} ->
                gui_jq:update(<<"users">>, users_table_expanded(SpaceId)),
                gui_comet:flush(),
                State;

            {user_row_collapse, SpaceId, UserId, RowId} ->
                gui_jq:update(RowId, user_row_collapsed(SpaceId, UserId, RowId)),
                gui_comet:flush(),
                State;

            {user_row_expand, SpaceId, UserId, RowId} ->
                gui_jq:update(RowId, user_row_expanded(SpaceId, UserId, RowId)),
                gui_comet:flush(),
                State;

            {groups_table_collapse, SpaceId} ->
                gui_jq:update(<<"groups">>, groups_table_collapsed(SpaceId)),
                gui_comet:flush(),
                State;

            {groups_table_expand, SpaceId} ->
                gui_jq:update(<<"groups">>, groups_table_expanded(SpaceId)),
                gui_comet:flush(),
                State;

            {group_row_collapse, SpaceId, GroupId, RowId} ->
                gui_jq:update(RowId, group_row_collapsed(SpaceId, GroupId, RowId)),
                gui_comet:flush(),
                State;

            {group_row_expand, SpaceId, GroupId, RowId} ->
                gui_jq:update(RowId, group_row_expanded(SpaceId, GroupId, RowId)),
                gui_comet:flush(),
                State
        end
               catch Type:Reason ->
                   ?error("Comet process exception: ~p:~p", [Type, Reason]),
                   message(<<"error_message">>, <<"There has been an error in comet process. Please refresh the page.">>),
                   {error, Reason}
               end,
    ?MODULE:comet_loop(NewState).


%% event/1
%% ====================================================================
%% @doc Handles page events.
-spec event(Event :: term()) -> no_return().
%% ====================================================================
event(init) ->
    SpaceId = gui_str:to_binary(gui_ctx:url_param(<<"id">>)),
    {ok, Pid} = gui_comet:spawn(fun() -> comet_loop(#?STATE{spaceId = SpaceId}) end),
    put(?COMET_PID, Pid),
    bind_key_to_click(<<"13">>, <<"button.confirm">>),
    gui_jq:update(<<"providers">>, providers_table_collapsed(SpaceId)),
    gui_jq:update(<<"users">>, users_table_collapsed(SpaceId)),
    gui_jq:update(<<"groups">>, groups_table_collapsed(SpaceId));

event({providers_table_collapse, SpaceId, SpinnerId}) ->
    get(?COMET_PID) ! {providers_table_collapse, SpaceId},
    gui_jq:update(SpinnerId, spinner());

event({providers_table_expand, SpaceId, SpinnerId}) ->
    get(?COMET_PID) ! {providers_table_expand, SpaceId},
    gui_jq:update(SpinnerId, spinner());

event({provider_row_collapse, SpaceId, ProviderId, RowId, SpinnerId}) ->
    get(?COMET_PID) ! {provider_row_collapse, SpaceId, ProviderId, RowId},
    gui_jq:update(SpinnerId, spinner());

event({provider_row_expand, SpaceId, ProviderId, RowId, SpinnerId}) ->
    get(?COMET_PID) ! {provider_row_expand, SpaceId, ProviderId, RowId},
    gui_jq:update(SpinnerId, spinner());

event({users_table_collapse, SpaceId, SpinnerId}) ->
    get(?COMET_PID) ! {users_table_collapse, SpaceId},
    gui_jq:update(SpinnerId, spinner());

event({users_table_expand, SpaceId, SpinnerId}) ->
    get(?COMET_PID) ! {users_table_expand, SpaceId},
    gui_jq:update(SpinnerId, spinner());

event({user_row_collapse, SpaceId, UserId, RowId, SpinnerId}) ->
    get(?COMET_PID) ! {user_row_collapse, SpaceId, UserId, RowId},
    gui_jq:update(SpinnerId, spinner());

event({user_row_expand, SpaceId, UserId, RowId, SpinnerId}) ->
    get(?COMET_PID) ! {user_row_expand, SpaceId, UserId, RowId},
    gui_jq:update(SpinnerId, spinner());

event({groups_table_collapse, SpaceId, SpinnerId}) ->
    get(?COMET_PID) ! {groups_table_collapse, SpaceId},
    gui_jq:update(SpinnerId, spinner());

event({groups_table_expand, SpaceId, SpinnerId}) ->
    get(?COMET_PID) ! {groups_table_expand, SpaceId},
    gui_jq:update(SpinnerId, spinner());

event({group_row_collapse, SpaceId, GroupId, RowId, SpinnerId}) ->
    get(?COMET_PID) ! {group_row_collapse, SpaceId, GroupId, RowId},
    gui_jq:update(SpinnerId, spinner());

event({group_row_expand, SpaceId, GroupId, RowId, SpinnerId}) ->
    get(?COMET_PID) ! {group_row_expand, SpaceId, GroupId, RowId},
    gui_jq:update(SpinnerId, spinner());

event({request_support, SpaceId}) ->
    get(?COMET_PID) ! {request_support, SpaceId},
    gui_jq:show(<<"main_spinner">>);

event({invite_user, SpaceId}) ->
    get(?COMET_PID) ! {invite_user, SpaceId},
    gui_jq:show(<<"main_spinner">>);

event({change_space_name, SpaceId, Name}) ->
    gui_jq:update(<<"space_name">>, change_space_name(SpaceId, Name)),
    gui_jq:bind_enter_to_submit_button(<<"new_space_name_textbox">>, <<"new_space_name_submit">>),
    gui_jq:focus(<<"new_space_name_textbox">>);

event({cancel_new_space_name_submit, SpaceId, Name}) ->
    gui_jq:update(<<"space_name">>, space_name(SpaceId, Name));

event({submit_new_space_name, SpaceId, Name}) ->
    NewUsername = gui_ctx:postback_param(<<"new_space_name_textbox">>),
    {UserGID, AccessToken} = gui_ctx:get_access_token(),
    case user_logic:get_user({global_id, UserGID}) of
        {ok, UserDoc} ->
            case gr_adapter:change_space_name(SpaceId, NewUsername, gui_ctx:get_access_token()) of
                ok ->
                    user_logic:synchronize_spaces_info(UserDoc, AccessToken),
                    gui_jq:update(<<"space_name">>, space_name(SpaceId, NewUsername));
                _ ->
                    message(<<"error_message">>, <<"Cannot change Space name.">>),
                    gui_jq:update(<<"space_name">>, space_name(SpaceId, Name))
            end;
        _ ->
            message(<<"error_message">>, <<"Cannot change Space name.">>),
            gui_jq:update(<<"space_name">>, space_name(SpaceId, Name))
    end;

event({close_message, MessageId}) ->
    gui_jq:hide(MessageId);

event(terminate) ->
    ok.