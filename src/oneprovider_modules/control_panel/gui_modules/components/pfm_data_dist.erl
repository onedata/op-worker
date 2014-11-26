%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains code for file_manager's data distribution view component.
%% It consists of several functions that render n2o elements and implement logic.
%% @end
%% ===================================================================
-module(pfm_data_dist).

-include("oneprovider_modules/control_panel/common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([init/3, data_distribution_panel/2, toggle_advanced_view/3, comet_loop_init/2]).

% Macros used to generate IDs of certain elements
-define(DIST_PANEL_ID(RowID), <<"dd_", (integer_to_binary(RowID))/binary>>).
-define(SHOW_DIST_PANEL_ID(RowID), <<"show_dd_", (integer_to_binary(RowID))/binary>>).
-define(HIDE_DIST_PANEL_ID(RowID), <<"hide_dd_", (integer_to_binary(RowID))/binary>>).
-define(CANVAS_ID(RowID, ProviderID), <<"canvas_", (integer_to_binary(RowID))/binary, "_", ProviderID/binary>>).
-define(SYNC_BUTTON_ID(RowID, ProviderID), <<"sync_", (integer_to_binary(RowID))/binary, "_", ProviderID/binary>>).
-define(EXPEL_BUTTON_ID(RowID, ProviderID), <<"expel_", (integer_to_binary(RowID))/binary, "_", ProviderID/binary>>).

% Reference to comet pid that handles updates of data distribution view
-define(DD_COMET_PID, dd_comet_pid).


%% ====================================================================
%% API functions
%% ====================================================================

init(GRUID, AccessToken, MainCometPid) ->
    {ok, Pid} = gui_comet:spawn(fun() -> comet_loop_init(GRUID, AccessToken) end),
    % Store DDCometPid in file manager's main comet process's memory
    MainCometPid ! {put, ?DD_COMET_PID, Pid}.


data_distribution_panel(FullPath, RowID) ->
    ShowAdvancedID = ?SHOW_DIST_PANEL_ID(RowID),
    % Item won't hightlight if the link is clicked.
    gui_jq:bind_element_click(ShowAdvancedID, <<"function(e) { e.stopPropagation(); }">>),
    HideAdvancedID = ?HIDE_DIST_PANEL_ID(RowID),
    % Item won't hightlight if the link is clicked.
    gui_jq:bind_element_click(HideAdvancedID, <<"function(e) { e.stopPropagation(); }">>),
    AdvancedPanelID = ?DIST_PANEL_ID(RowID),
    [
        #link{id = ShowAdvancedID, postback = {action, ?MODULE, toggle_advanced_view, [FullPath, RowID, true]},
            title = <<"Data distribution (advanced)">>, class = <<"glyph-link hidden show-on-parent-hover ddist-show-button">>,
            body = #span{class = <<"icomoon-earth">>}},
        #link{id = HideAdvancedID, postback = {action, ?MODULE, toggle_advanced_view, [FullPath, RowID, false]},
            title = <<"Hide data distribution view">>, class = <<"glyph-link hidden ddist-hide-button">>,
            body = #span{class = <<"icomoon-minus4">>}},
        #panel{class = <<"clearfix">>},
        #panel{id = AdvancedPanelID, class = <<"ddist-panel">>, body = <<"">>}
    ].


toggle_advanced_view(FullPath, RowID, Flag) ->
    ShowAdvancedID = ?SHOW_DIST_PANEL_ID(RowID),
    HideAdvancedID = ?HIDE_DIST_PANEL_ID(RowID),
    AdvancedPanelID = ?DIST_PANEL_ID(RowID),
    case Flag of
        true ->
            gui_jq:update(AdvancedPanelID, render_table(FullPath, RowID)),
            gui_jq:add_class(HideAdvancedID, <<"show-on-parent-hover">>),
            gui_jq:remove_class(ShowAdvancedID, <<"show-on-parent-hover">>),
            gui_jq:slide_down(AdvancedPanelID, 400);
        false ->
            gui_jq:add_class(ShowAdvancedID, <<"show-on-parent-hover">>),
            gui_jq:remove_class(HideAdvancedID, <<"show-on-parent-hover">>),
            gui_jq:slide_up(AdvancedPanelID, 200)
    end,
    get(?DD_COMET_PID) ! {toggle_watching, FullPath, RowID, Flag}.


refresh_view(FullPath, RowID) ->
    ?dump({refresh, ?DIST_PANEL_ID(RowID)}),
    gui_jq:update(?DIST_PANEL_ID(RowID), render_table(FullPath, RowID)).


render_table(FilePath, RowID) ->
    {FileSize, Blocks} = fs_interface:file_parts_mock(FilePath),
    Table = #table{class = <<"ddist-table">>,
        body = #tbody{body = [
            lists:map(
                fun({ProviderID, ProvBytes, BlockList}) ->
                    CanvasID = ?CANVAS_ID(RowID, ProviderID),
                    SyncButtonID = ?SYNC_BUTTON_ID(RowID, ProviderID),
                    ExpelButtonID = ?EXPEL_BUTTON_ID(RowID, ProviderID),
                    JSON = rest_utils:encode_to_json([{<<"file_size">>, FileSize}, {<<"chunks">>, BlockList}]),
                    gui_jq:wire(<<"new FileChunksBar(document.getElementById('", CanvasID/binary, "'), '", JSON/binary, "');">>),
                    #tr{cells = [
                        #td{body = ProviderID, class = <<"ddist-provider">>},
                        #td{body = gui_str:format_bin("~.2f%", [ProvBytes * 100 / FileSize])},
                        #td{body = #canvas{id = CanvasID, class = <<"ddist-canvas">>}},
                        #td{body = #link{id = SyncButtonID, postback = {action, ?MODULE, sync_file, [FilePath]},
                            title = <<"Issue full synchronization">>, class = <<"glyph-link ddist-button">>,
                            body = #span{class = <<"icomoon-spinner6">>}}},
                        #td{body = #link{id = SyncButtonID, postback = {action, ?MODULE, sync_file, [FilePath]},
                            title = <<"Expel all chunks from this provider">>, class = <<"glyph-link ddist-button">>,
                            body = #span{class = <<"icomoon-blocked">>}}}
                    ]}
                end, Blocks)
        ]}
    }.


comet_loop_init(GRUID, UserAccessToken) ->
    % Initialize context
    fslogic_context:set_gr_auth(GRUID, UserAccessToken),
    gui_comet:flush(),
    comet_loop([]).


comet_loop(WatchedFiles) ->
    NewWatchedFiles = receive
                          {toggle_watching, FullPath, RowID, Flag} ->
                              case Flag of
                                  true ->
                                      [{FullPath, RowID} | WatchedFiles];
                                  false ->
                                      WatchedFiles -- [{FullPath, RowID}]
                              end
                      after
                          50000 ->
                              % TODO mock
                              lists:foreach(
                                  fun({FullPath, RowID}) ->
                                      refresh_view(FullPath, RowID)
                                  end, WatchedFiles),
                              gui_comet:flush(),
                              WatchedFiles
                      end,
    comet_loop(NewWatchedFiles).
