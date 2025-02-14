#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/functional.h>
#include <vector>
#include <unordered_set>
#include <unordered_map>
#include <string>
#include <chrono>
#include <queue>
#include <mutex>
#include <atomic>
#include <nlohmann/json.hpp>
#include <iostream>
#include <fstream>

namespace py = pybind11;
using json = nlohmann::json;

struct Flight {
    std::string origin;
    std::string destination;
    std::string key;
    int64_t departure;
    int64_t arrival;
    double cost;
    std::string currency;
};

class PathFinder {
private:
    std::unordered_map<std::string, std::vector<Flight>> flights_by_origin;
    std::string origin;
    int min_nights;
    int max_nights;
    int max_flights;
    std::atomic<bool> stop{false};
    std::mutex cout_mutex;
    std::mutex callback_mutex;
    std::atomic<size_t> paths_found{0};
    py::function callback;

    void log(const std::string& msg) {
        std::lock_guard<std::mutex> lock(cout_mutex);
        std::cout << msg << std::endl;
    }

    bool is_valid_stay(int64_t arrival, int64_t next_departure) const {
        int64_t stay_hours = (next_departure - arrival) / 3600;
        if (min_nights == 0) {
            return stay_hours >= 2; // Minimum 2 hour connection
        }
        int64_t stay_nights = (next_departure - arrival) / 86400;
        return stay_nights >= min_nights && stay_nights <= max_nights;
    }

    void process_path(const std::vector<Flight>& path) {
        if (stop) return;

        try {
            py::gil_scoped_acquire acquire;
            
            // Create Python list to hold the path
            py::list py_path;
            
            // Convert each flight to a Python dictionary
            for (const auto& flight : path) {
                py::dict flight_dict;
                flight_dict["origin"] = flight.origin;
                flight_dict["destination"] = flight.destination;
                flight_dict["key"] = flight.key;
                flight_dict["departure"] = flight.departure;
                flight_dict["arrival"] = flight.arrival;
                flight_dict["cost"] = flight.cost;
                flight_dict["currency"] = flight.currency;
                py_path.append(flight_dict);
            }
            
            // Call the Python callback with the path
            {
                std::lock_guard<std::mutex> lock(callback_mutex);
                callback(py_path);
            }
            
            paths_found++;
            if (paths_found % 1000 == 0) {
                log("Found " + std::to_string(paths_found) + " paths");
            }
            
        } catch (const py::error_already_set& e) {
            log("Python callback error: " + std::string(e.what()));
            if (PyErr_Occurred()) {
                PyErr_Print();
            }
        } catch (const std::exception& e) {
            log("Error in process_path: " + std::string(e.what()));
        }
    }

    void find_paths(const std::vector<Flight>& current_path, 
                   const std::unordered_set<std::string>& visited,
                   size_t depth = 0) {
        if (stop || depth >= max_flights) return;

        const auto& last_flight = current_path.back();
        const auto& curr_location = last_flight.destination;

        // Check if we can return to origin
        if (depth >= 1 && curr_location != origin) {
            auto it = flights_by_origin.find(curr_location);
            if (it != flights_by_origin.end()) {
                for (const auto& return_flight : it->second) {
                    if (stop) return;
                    
                    if (return_flight.destination == origin && 
                        return_flight.departure > last_flight.arrival &&
                        is_valid_stay(last_flight.arrival, return_flight.departure)) {
                        
                        std::vector<Flight> complete_path = current_path;
                        complete_path.push_back(return_flight);
                        process_path(complete_path);
                    }
                }
            }
        }

        // Continue searching
        if (depth + 1 < max_flights) {
            auto it = flights_by_origin.find(curr_location);
            if (it != flights_by_origin.end()) {
                for (const auto& next_flight : it->second) {
                    if (stop) return;
                    
                    if (next_flight.destination != origin && 
                        visited.count(next_flight.destination) == 0 &&
                        next_flight.departure > last_flight.arrival &&
                        is_valid_stay(last_flight.arrival, next_flight.departure)) {
                        
                        std::vector<Flight> new_path = current_path;
                        new_path.push_back(next_flight);
                        
                        std::unordered_set<std::string> new_visited = visited;
                        new_visited.insert(next_flight.destination);
                        
                        find_paths(new_path, new_visited, depth + 1);
                    }
                }
            }
        }
    }

public:
    PathFinder(const std::string& origin, int min_nights, int max_nights, int max_flights)
        : origin(origin), min_nights(min_nights), max_nights(max_nights), max_flights(max_flights) {}

    void load_graph(const std::string& filepath) {
        std::ifstream file(filepath);
        if (!file.is_open()) {
            throw std::runtime_error("Could not open graph file: " + filepath);
        }

        json graph_json;
        file >> graph_json;

        for (const auto& [from, edges] : graph_json.items()) {
            auto& flights = flights_by_origin[from];
            flights.reserve(edges.size());
            
            for (const auto& edge : edges) {
                Flight flight;
                flight.origin = from;
                flight.destination = edge["to"];
                flight.key = edge["key"];
                flight.departure = edge["departure"];
                flight.arrival = edge["arrival"];
                flight.cost = edge["weight"];
                flight.currency = edge["currency"];
                flights.push_back(flight);
            }
        }
        
        log("Loaded graph with " + std::to_string(flights_by_origin.size()) + " nodes");
    }

    size_t process(const py::function& path_callback) {
        if (!path_callback) {
            throw std::runtime_error("Invalid callback function");
        }

        callback = path_callback;
        paths_found = 0;
        stop = false;

        // Release GIL during the main search
        py::gil_scoped_release release;

        auto initial_flights = flights_by_origin[origin];
        log("Starting search with " + std::to_string(initial_flights.size()) + " initial flights");

        for (const auto& first_flight : initial_flights) {
            if (stop) break;
            
            std::vector<Flight> initial_path = {first_flight};
            std::unordered_set<std::string> visited = {origin, first_flight.destination};
            find_paths(initial_path, visited);
        }

        return paths_found;
    }

    void stop_processing() {
        stop = true;
    }
};

PYBIND11_MODULE(path_finder, m) {
    py::class_<PathFinder>(m, "PathFinder")
        .def(py::init<const std::string&, int, int, int>())
        .def("load_graph", &PathFinder::load_graph)
        .def("process", &PathFinder::process)
        .def("stop_processing", &PathFinder::stop_processing);
} 