import com.vanniktech.maven.publish.SonatypeHost

plugins {
    alias(libs.plugins.kotlinMultiplatform)
    alias(libs.plugins.vanniktech.mavenPublish)
    alias(libs.plugins.kotlinx.atomicfu)
}

group = "io.github.kotlin"
version = "1.0.0"

kotlin {
    jvm()

    js {
        nodejs()
    }

    @OptIn(org.jetbrains.kotlin.gradle.ExperimentalWasmDsl::class)
    wasmJs {
        nodejs()
    }

    // TODO enable after https://youtrack.jetbrains.com/issue/KT-65179 is resolved
//    @OptIn(org.jetbrains.kotlin.gradle.ExperimentalWasmDsl::class)
//    wasmWasi {
//        nodejs()
//
//        binaries.library()
//    }

    listOf(
        iosX64(),
        iosArm64(),
        iosSimulatorArm64()
    ).forEach {
        it.binaries.framework {
            baseName = "bitcask-mp"
            isStatic = true
        }
    }

    mingwX64()
    linuxX64()

    sourceSets {
        commonMain.dependencies {
            implementation(libs.kotlinx.io.core)
            implementation(libs.kotlin.logging)
        }

        jvmMain.dependencies {
            implementation(libs.logging.log4j)
        }

        commonTest.dependencies {
            implementation(libs.kotlin.test)
        }
    }
}

publishing {
    repositories {
        maven {
            name = "githubPackages"
            url = uri("https://maven.pkg.github.com/przemo199/bitcask-mp")
            credentials(PasswordCredentials::class)
        }
    }
}

mavenPublishing {
    publishToMavenCentral(SonatypeHost.CENTRAL_PORTAL)

    signAllPublications()

    coordinates(
        groupId = group.toString(),
        artifactId = "library",
        version = version.toString()
    )

    pom {
        name = "bitcask-mp"
        description = "Bitcask multiplatform library"
        inceptionYear = "2024"
        url = "https://github.com/przemo199/bitcask-mp/"

        licenses {
            license {
                name = "Apache-2.0"
                url = "https://www.apache.org/licenses/LICENSE-2.0.txt"
                distribution = "repo"
            }
        }

        developers {
            developer {
                id = "przemo199"
                name = "Przemek Kamiński"
                url = "https://github.com/przemo199"
            }
        }
    }
}
