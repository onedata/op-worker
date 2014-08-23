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
-include_lib("ctool/include/global_registry/gr_users.hrl").
-include_lib("ctool/include/global_registry/gr_spaces.hrl").
-include_lib("ctool/include/global_registry/gr_groups.hrl").
-include_lib("ctool/include/global_registry/gr_providers.hrl").
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
            case gr_users:get_space_details({user, vcn_gui_utils:get_access_token()}, SpaceId) of
                {ok, SpaceDetails} ->
                    space(SpaceDetails);
                _ ->
                    page_error:redirect_with_error(?error_space_permission_denied),
                    []
            end
    end.


%% space/1
%% ====================================================================
%% @doc Renders Space settings.
-spec space(SpaceDetails :: #space_details{}) -> [#panel{}].
%% ====================================================================
space(#space_details{id = SpaceId, name = Name}) ->
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
                                    body = vcn_gui_utils:spinner()
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
                        class = <<"btn btn-primary btn-small">>,
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
                                    body = vcn_gui_utils:spinner()
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
                        class = <<"btn btn-primary btn-small">>,
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
                                    body = vcn_gui_utils:spinner()
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
                body = vcn_gui_utils:expand_button(<<"Expand All">>, {providers_table_expand, SpaceId, SpinnerId})
            }
        ]
    },
    try
        {ok, ProviderIds} = gr_spaces:get_providers({user, vcn_gui_utils:get_access_token()}, SpaceId),
        Rows = lists:map(fun({ProviderId, Counter}) ->
            RowId = <<"provider_", (integer_to_binary(Counter))/binary>>,
            #tr{
                id = RowId,
                cells = provider_row_collapsed(SpaceId, ProviderId, RowId)
            }
        end, lists:zip(ProviderIds, tl(lists:seq(0, length(ProviderIds))))),
        [Header | Rows]
    catch
        _:Reason ->
            ?error("Cannot fetch providers of Space with ID: ~p: ~p", [SpaceId, Reason]),
            vcn_gui_utils:message(<<"error_message">>, <<"Cannot fetch providers of Space with ID: <b>", SpaceId/binary, "</b>."
            "<br>Please try again later.">>),
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
                body = vcn_gui_utils:collapse_button(<<"Collapse All">>, {providers_table_collapse, SpaceId, SpinnerId})
            }
        ]
    },
    try
        {ok, ProviderIds} = gr_spaces:get_providers({user, vcn_gui_utils:get_access_token()}, SpaceId),
        Rows = lists:map(fun({ProviderId, Counter}) ->
            RowId = <<"provider_", (integer_to_binary(Counter))/binary>>,
            #tr{
                id = RowId,
                cells = provider_row_expanded(SpaceId, ProviderId, RowId)
            }
        end, lists:zip(ProviderIds, tl(lists:seq(0, length(ProviderIds))))),
        [Header | Rows]
    catch
        _:Reason ->
            ?error("Cannot fetch providers of Space with ID: ~p: ~p", [SpaceId, Reason]),
            vcn_gui_utils:message(<<"error_message">>, <<"Cannot fetch providers of Space with ID: <b>", SpaceId/binary, "</b>."
            "<br>Please try again later.">>),
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
            body = vcn_gui_utils:expand_button({provider_row_expand, SpaceId, ProviderId, RowId, SpinnerId})
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
        {ok, #provider_details{urls = URLs, redirection_point = RedirectionPoint}} =
            gr_spaces:get_provider_details({user, vcn_gui_utils:get_access_token()}, SpaceId, ProviderId),
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
                                            body = lists:map(fun(URL) ->
                                                #li{body = #p{
                                                    style = ?PARAGRAPH_STYLE,
                                                    body = URL}
                                                }
                                            end, URLs)
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
                body = vcn_gui_utils:collapse_button({provider_row_collapse, SpaceId, ProviderId, RowId, SpinnerId})
            }
        ]
    catch
        _:Reason ->
            ?error("Cannot fetch details of provider with ID: ~p: ~p", [ProviderId, Reason]),
            vcn_gui_utils:message(<<"error_message">>, <<"Cannot fetch details of provider with ID: <b>", ProviderId/binary, "</b>."
            "<br>Please try again later.">>),
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
                body = vcn_gui_utils:expand_button(<<"Expand All">>, {users_table_expand, SpaceId, SpinnerId})
            }
        ]
    },
    try
        {ok, UserIds} = gr_spaces:get_users({user, vcn_gui_utils:get_access_token()}, SpaceId),
        Rows = lists:map(fun({UserId, Counter}) ->
            RowId = <<"user_", (integer_to_binary(Counter))/binary>>,
            #tr{
                id = RowId,
                cells = user_row_collapsed(SpaceId, UserId, RowId)
            }
        end, lists:zip(UserIds, tl(lists:seq(0, length(UserIds))))),
        [Header | Rows]
    catch
        _:Reason ->
            ?error("Cannot fetch users of Space with ID: ~p: ~p", [SpaceId, Reason]),
            vcn_gui_utils:message(<<"error_message">>, <<"Cannot fetch users of Space with ID: <b>", SpaceId/binary, "</b>."
            "<br>Please try again later.">>),
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
                body = vcn_gui_utils:collapse_button(<<"Collapse All">>, {users_table_collapse, SpaceId, SpinnerId})
            }
        ]
    },
    try
        {ok, UserIds} = gr_spaces:get_users({user, vcn_gui_utils:get_access_token()}, SpaceId),
        Rows = lists:map(fun({UserId, Counter}) ->
            RowId = <<"user_", (integer_to_binary(Counter))/binary>>,
            #tr{
                id = RowId,
                cells = user_row_expanded(SpaceId, UserId, RowId)
            }
        end, lists:zip(UserIds, tl(lists:seq(0, length(UserIds))))),
        [Header | Rows]
    catch
        _:Reason ->
            ?error("Cannot fetch users of Space with ID: ~p: ~p", [SpaceId, Reason]),
            vcn_gui_utils:message(<<"error_message">>, <<"Cannot fetch users of Space with ID: <b>", SpaceId/binary, "</b>."
            "<br>Please try again later.">>),
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
        {ok, #user_details{name = Name}} = gr_spaces:get_user_details({user, vcn_gui_utils:get_access_token()}, SpaceId, UserId),
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
                body = vcn_gui_utils:expand_button({user_row_expand, SpaceId, UserId, RowId, SpinnerId})
            }
        ]
    catch
        _:Reason ->
            ?error("Cannot fetch details of users with ID: ~p: ~p", [UserId, Reason]),
            vcn_gui_utils:message(<<"error_message">>, <<"Cannot fetch details of user with ID: <b>", UserId/binary, "</b>."
            "<br>Please try again later.">>),
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
        {ok, #user_details{name = Name}} = gr_spaces:get_user_details({user, vcn_gui_utils:get_access_token()}, SpaceId, UserId),
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
                body = vcn_gui_utils:collapse_button({user_row_collapse, SpaceId, UserId, RowId, SpinnerId})
            }
        ]
    catch
        _:Reason ->
            ?error("Cannot fetch details of users with ID: ~p: ~p", [UserId, Reason]),
            vcn_gui_utils:message(<<"error_message">>, <<"Cannot fetch details of user with ID: <b>", UserId/binary, "</b>."
            "<br>Please try again later.">>),
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
                body = vcn_gui_utils:expand_button(<<"Expand All">>, {groups_table_expand, SpaceId, SpinnerId})
            }
        ]
    },
    try
        {ok, GroupIds} = gr_spaces:get_groups({user, vcn_gui_utils:get_access_token()}, SpaceId),
        Rows = lists:map(fun({GroupId, Counter}) ->
            RowId = <<"group_", (integer_to_binary(Counter))/binary>>,
            #tr{
                id = RowId,
                cells = group_row_collapsed(SpaceId, GroupId, RowId)
            }
        end, lists:zip(GroupIds, tl(lists:seq(0, length(GroupIds))))),
        [Header | Rows]
    catch
        _:Reason ->
            ?error("Cannot fetch groups of Space with ID: ~p: ~p", [SpaceId, Reason]),
            vcn_gui_utils:message(<<"error_message">>, <<"Cannot fetch groups of Space with ID: <b>", SpaceId/binary, "</b>."
            "<br>Please try again later.">>),
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
                body = vcn_gui_utils:collapse_button(<<"Collapse All">>, {groups_table_collapse, SpaceId, SpinnerId})
            }
        ]
    },
    try
        {ok, GroupIds} = gr_spaces:get_groups({user, vcn_gui_utils:get_access_token()}, SpaceId),
        Rows = lists:map(fun({GroupId, Counter}) ->
            RowId = <<"group_", (integer_to_binary(Counter))/binary>>,
            #tr{
                id = RowId,
                cells = group_row_expanded(SpaceId, GroupId, RowId)
            }
        end, lists:zip(GroupIds, tl(lists:seq(0, length(GroupIds))))),
        [Header | Rows]
    catch
        _:Reason ->
            ?error("Cannot fetch groups of Space with ID: ~p: ~p", [SpaceId, Reason]),
            vcn_gui_utils:message(<<"error_message">>, <<"Cannot fetch groups of Space with ID: <b>", SpaceId/binary, "</b>."
            "<br>Please try again later.">>),
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
        {ok, #group_details{name = Name}} = gr_spaces:get_group_details({user, vcn_gui_utils:get_access_token()}, SpaceId, GroupId),
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
                body = vcn_gui_utils:expand_button({group_row_expand, SpaceId, GroupId, RowId, SpinnerId})
            }
        ]
    catch
        _:Reason ->
            ?error("Cannot fetch details of group with ID: ~p: ~p", [GroupId, Reason]),
            vcn_gui_utils:message(<<"error_message">>, <<"Cannot fetch details of group with ID: <b>", GroupId/binary, "</b>."
            "<br>Please try again later.">>),
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
        {ok, #group_details{name = Name}} = gr_spaces:get_group_details({user, vcn_gui_utils:get_access_token()}, SpaceId, GroupId),
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
                body = vcn_gui_utils:collapse_button({group_row_collapse, SpaceId, GroupId, RowId, SpinnerId})
            }
        ]
    catch
        _:Reason ->
            ?error("Cannot fetch details of group with ID: ~p: ~p", [GroupId, Reason]),
            vcn_gui_utils:message(<<"error_message">>, <<"Cannot fetch details of group with ID: <b>", GroupId/binary, "</b>."
            "<br>Please try again later.">>),
            []
    end.


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
                case gr_spaces:get_invite_provider_token({user, vcn_gui_utils:get_access_token()}, SpaceId) of
                    {ok, Token} ->
                        Message = <<"Give underlying token to any Provider that is willing to support your Space.",
                        "<input type=\"text\" style=\"margin-top: 1em; width: 80%;\" value=\"", Token/binary, "\">">>,
                        gui_jq:info_popup(<<"Request support">>, Message, <<"return true;">>),
                        gui_comet:flush();
                    _ ->
                        vcn_gui_utils:message(<<"error_message">>, <<"Cannot get Space support token.<br>Please try again later.">>)
                end,
                gui_jq:hide(<<"main_spinner">>),
                gui_comet:flush(),
                State;

            {invite_user, SpaceId} ->
                case gr_spaces:get_invite_user_token({user, vcn_gui_utils:get_access_token()}, SpaceId) of
                    {ok, Token} ->
                        Message = <<"Give underlying token to any User that is willing to join your Space.",
                        "<input type=\"text\" style=\"margin-top: 1em; width: 80%;\" value=\"", Token/binary, "\">">>,
                        gui_jq:info_popup(<<"Invite user">>, Message, <<"return true;">>),
                        gui_comet:flush();
                    _ ->
                        vcn_gui_utils:message(<<"error_message">>, <<"Cannot get Space invitation token.<br>Please try again later.">>)
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
                   vcn_gui_utils:message(<<"error_message">>, <<"There has been an error in comet process. Please refresh the page.">>),
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
    gui_jq:bind_key_to_click_on_class(<<"13">>, <<"button.confirm">>),
    gui_jq:update(<<"providers">>, providers_table_collapsed(SpaceId)),
    gui_jq:update(<<"users">>, users_table_collapsed(SpaceId)),
    gui_jq:update(<<"groups">>, groups_table_collapsed(SpaceId));

event({providers_table_collapse, SpaceId, SpinnerId}) ->
    get(?COMET_PID) ! {providers_table_collapse, SpaceId},
    gui_jq:update(SpinnerId, vcn_gui_utils:spinner());

event({providers_table_expand, SpaceId, SpinnerId}) ->
    get(?COMET_PID) ! {providers_table_expand, SpaceId},
    gui_jq:update(SpinnerId, vcn_gui_utils:spinner());

event({provider_row_collapse, SpaceId, ProviderId, RowId, SpinnerId}) ->
    get(?COMET_PID) ! {provider_row_collapse, SpaceId, ProviderId, RowId},
    gui_jq:update(SpinnerId, vcn_gui_utils:spinner());

event({provider_row_expand, SpaceId, ProviderId, RowId, SpinnerId}) ->
    get(?COMET_PID) ! {provider_row_expand, SpaceId, ProviderId, RowId},
    gui_jq:update(SpinnerId, vcn_gui_utils:spinner());

event({users_table_collapse, SpaceId, SpinnerId}) ->
    get(?COMET_PID) ! {users_table_collapse, SpaceId},
    gui_jq:update(SpinnerId, vcn_gui_utils:spinner());

event({users_table_expand, SpaceId, SpinnerId}) ->
    get(?COMET_PID) ! {users_table_expand, SpaceId},
    gui_jq:update(SpinnerId, vcn_gui_utils:spinner());

event({user_row_collapse, SpaceId, UserId, RowId, SpinnerId}) ->
    get(?COMET_PID) ! {user_row_collapse, SpaceId, UserId, RowId},
    gui_jq:update(SpinnerId, vcn_gui_utils:spinner());

event({user_row_expand, SpaceId, UserId, RowId, SpinnerId}) ->
    get(?COMET_PID) ! {user_row_expand, SpaceId, UserId, RowId},
    gui_jq:update(SpinnerId, vcn_gui_utils:spinner());

event({groups_table_collapse, SpaceId, SpinnerId}) ->
    get(?COMET_PID) ! {groups_table_collapse, SpaceId},
    gui_jq:update(SpinnerId, vcn_gui_utils:spinner());

event({groups_table_expand, SpaceId, SpinnerId}) ->
    get(?COMET_PID) ! {groups_table_expand, SpaceId},
    gui_jq:update(SpinnerId, vcn_gui_utils:spinner());

event({group_row_collapse, SpaceId, GroupId, RowId, SpinnerId}) ->
    get(?COMET_PID) ! {group_row_collapse, SpaceId, GroupId, RowId},
    gui_jq:update(SpinnerId, vcn_gui_utils:spinner());

event({group_row_expand, SpaceId, GroupId, RowId, SpinnerId}) ->
    get(?COMET_PID) ! {group_row_expand, SpaceId, GroupId, RowId},
    gui_jq:update(SpinnerId, vcn_gui_utils:spinner());

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

event({submit_new_space_name, SpaceId, Name}) ->
    NewSpaceName = gui_ctx:postback_param(<<"new_space_name_textbox">>),
    GRUID = vcn_gui_utils:get_global_user_id(),
    AccessToken = vcn_gui_utils:get_access_token(),
    ?dump(GRUID),
    ?dump(AccessToken),
    case user_logic:get_user({global_id, GRUID}) of
        {ok, UserDoc} ->
            case gr_spaces:modify_details({user, vcn_gui_utils:get_access_token()}, SpaceId, [{<<"name">>, NewSpaceName}]) of
                ok ->
                    user_logic:synchronize_spaces_info(UserDoc, AccessToken),
                    gui_jq:update(<<"space_name">>, space_name(SpaceId, NewSpaceName));
                Other ->
                    ?error("Cannot change name of Space with ID ~p: ~p", [SpaceId, Other]),
                    vcn_gui_utils:message(<<"error_message">>, <<"Cannot change Space name.">>),
                    gui_jq:update(<<"space_name">>, space_name(SpaceId, Name))
            end;
        _ ->
            vcn_gui_utils:message(<<"error_message">>, <<"Cannot change Space name.">>),
            gui_jq:update(<<"space_name">>, space_name(SpaceId, Name))
    end;

event({cancel_new_space_name_submit, SpaceId, Name}) ->
    gui_jq:update(<<"space_name">>, space_name(SpaceId, Name));

event({close_message, MessageId}) ->
    gui_jq:hide(MessageId);

event(terminate) ->
    ok.