#include "folders_index_query.h"

int main() {
    auto client = gcs::Client();
    auto res = get_obj_name_to_trace_ids_map_from_folders_index("span.kind", "client", 1665612696, 1665612699, &client);
    if (!res.ok()) {
        std::cout << res.status() << std::endl;
    } else {
        std::cout << res->size() << std::endl;
    }
    // for (auto [key, val] : res) {
    //     std::cout << key << ": " << std::flush;
    //     for (auto ele : val) {
    //         std::cout << ele << ", " << std::flush;
    //     }
    //     std::cout << std::endl;
    // }
    return 0;
}
