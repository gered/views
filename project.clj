(defproject gered/views "1.6-SNAPSHOT"
  :description  "A view to the past helps navigate the future."
  :url          "https://github.com/gered/views"

  :license      {:name "MIT License"
                 :url "http://opensource.org/licenses/MIT"}

  :dependencies [[org.clojure/tools.logging "0.3.1"]
                 [prismatic/plumbing "0.5.3"]]

  :profiles     {:provided
                 {:dependencies [[org.clojure/clojure "1.8.0"]]}

                 :test
                 {:dependencies [[pjstadig/humane-test-output "0.8.0"]]
                  :injections   [(require 'pjstadig.humane-test-output)
                                 (pjstadig.humane-test-output/activate!)]}})
